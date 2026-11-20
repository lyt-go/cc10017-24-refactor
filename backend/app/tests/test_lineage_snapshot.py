"""血缘图构建收拢与版本快照测试。

覆盖：
- 重构前后节点输入、边输出与历史口径完全一致；
- 同一血缘版本重复构建只生成一个快照（幂等）；
- 写入中断后从最后成功位置恢复，不重复写入；
- 历史版本冲突冻结为新修订，旧快照仍可回查；
- 快照交付死信处理后，重放目标判断保持原口径；
- 同名节点统一查找口径。
"""
from __future__ import annotations

import unittest
from typing import Any

from app.lineage_graph import (
    DEFAULT_EDGE_LABEL,
    build_graph,
    build_version_graph,
    duplicate_names,
    find_node_by_name,
    graph_fingerprint,
    lineage_snapshot_store,
)
from app.services.dead_letter import DeadLetterService
from app.services.stream_lineage import StreamLineageService
from app.store import store


def _extra_lineage_row(version: str = "v1") -> dict[str, Any]:
    rows = store.rows("stream_lineage")
    return {
        "id": max(int(r.get("id", 0)) for r in rows) + 1,
        "status": "草稿", "pending": True, "abnormal": False,
        "记录编号": f"LINE-TEST-{len(rows)+1}", "上游主题": "TOPI-TEST",
        "下游表名": "test_downstream", "转换节点": "flink-test",
        "字段映射": "a->b", "采集时间": "2026-11-16T11:00:00",
        "延迟偏移": "1", "链路版本": version,
    }


class TestGraphHistoricalParity(unittest.TestCase):
    """重构前后图口径一致：节点输入与边输出。"""

    def test_graph_matches_legacy_shape(self) -> None:
        graph = StreamLineageService().get_graph()
        self.assertEqual([node["id"] for node in graph["nodes"]], [1, 2, 3])
        # 历史口径：name 取记录第二个键（种子数据即 status 文本）
        self.assertEqual([node["name"] for node in graph["nodes"]], ["已生效", "已生效", "草稿"])
        self.assertEqual(
            graph["edges"],
            [
                {"from": 1, "to": 2, "label": DEFAULT_EDGE_LABEL},
                {"from": 2, "to": 3, "label": DEFAULT_EDGE_LABEL},
            ],
        )

    def test_version_graph_equals_full_graph_for_seed_version(self) -> None:
        full = build_graph()
        versioned = build_version_graph("v1")
        self.assertEqual(graph_fingerprint(full), graph_fingerprint(versioned))

    def test_node_business_fields_preserved(self) -> None:
        graph = build_graph()
        first = graph["nodes"][0]
        self.assertEqual(first["记录编号"], "LINE-0001")
        self.assertEqual(first["上游主题"], "TOPI-0001")
        self.assertEqual(first["下游表名"], "order_agg_hourly")
        self.assertNotIn("pending", first)
        self.assertNotIn("abnormal", first)

    def test_same_name_lookup_uses_first_match(self) -> None:
        graph = {
            "nodes": [
                {"id": 1, "name": "dup", "下游表名": "first"},
                {"id": 2, "name": "dup", "下游表名": "second"},
            ]
        }
        node = find_node_by_name(graph, "dup")
        self.assertIsNotNone(node)
        self.assertEqual(node["id"], 1)  # 行序首条，统一旧 next(...) 口径
        self.assertIsNone(find_node_by_name(graph, "missing"))
        self.assertEqual(duplicate_names(graph), ["dup"])


class TestSnapshotVersioning(unittest.TestCase):
    """版本快照：冻结、幂等、冲突修订。"""

    def test_freeze_records_historical_caliber(self) -> None:
        snapshot = lineage_snapshot_store.ensure_snapshot("v1")
        self.assertTrue(snapshot["frozen"])
        self.assertEqual(snapshot["edge_policy"], "legacy_order")
        live = build_version_graph("v1")
        self.assertEqual(snapshot["node_count"], len(live["nodes"]))
        self.assertEqual(snapshot["edge_count"], len(live["edges"]))
        self.assertEqual(snapshot["fingerprint"], graph_fingerprint(live))

    def test_repeated_build_produces_single_snapshot(self) -> None:
        first = lineage_snapshot_store.ensure_snapshot("v1")
        snapshots_before = len(lineage_snapshot_store.list_snapshots("v1"))
        second = lineage_snapshot_store.ensure_snapshot("v1")
        third = lineage_snapshot_store.ensure_snapshot("v1")
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(second["id"], third["id"])
        # 重复构建不得新增快照行
        self.assertEqual(
            len(lineage_snapshot_store.list_snapshots("v1")), snapshots_before
        )
        self.assertEqual(
            sum(1 for snapshot in lineage_snapshot_store.list_snapshots("v1")
                if snapshot["id"] == first["id"]),
            1,
        )

    def test_historical_conflict_freezes_new_revision(self) -> None:
        before = lineage_snapshot_store.ensure_snapshot("v1")
        store.rows("stream_lineage").append(_extra_lineage_row())
        try:
            after = lineage_snapshot_store.ensure_snapshot("v1")
            self.assertNotEqual(before["id"], after["id"])
            self.assertEqual(after["revision"], before["revision"] + 1)
            # 旧快照仍可回查，节点边集合保持冻结时口径
            old = lineage_snapshot_store.get_snapshot("v1", before["revision"])
            self.assertEqual(old["node_count"], before["node_count"])
            self.assertEqual(old["edge_count"], before["edge_count"])
            self.assertEqual(lineage_snapshot_store.current_snapshot()["id"], after["id"])
            # 冲突解决后恢复数据，重新构建应回到与现有最新快照幂等
        finally:
            store.rows("stream_lineage").pop()

    def test_compare_reports_equal_node_and_edge_sets(self) -> None:
        lineage_snapshot_store.ensure_snapshot("v1")
        report = lineage_snapshot_store.compare_with_live("v1")
        self.assertTrue(report["nodes_equal"])
        self.assertTrue(report["edges_equal"])
        self.assertTrue(report["edge_order_preserved"])
        self.assertEqual(report["nodes_only_in_snapshot"], [])
        self.assertEqual(report["edges_only_in_live"], [])


class TestSnapshotWriteRecovery(unittest.TestCase):
    """写入中断后恢复：最后成功位置决定续传游标，避免重复写入。"""

    def test_interrupted_write_resumes_from_cursor(self) -> None:
        version = f"ut-resume-{id(self)}"
        first_row = _extra_lineage_row(version)
        second_row = _extra_lineage_row(version)
        second_row["id"] = first_row["id"] + 1
        second_row["记录编号"] = first_row["记录编号"] + "-B"
        store.rows("stream_lineage").extend([first_row, second_row])
        try:
            lineage_snapshot_store.arm_failure(version, 1)
            with self.assertRaises(RuntimeError):
                lineage_snapshot_store.ensure_snapshot(version)
            state = lineage_snapshot_store.get_write_state(version)
            self.assertEqual(state["status"], "interrupted")
            self.assertEqual(state["cursor"], 1)
            self.assertEqual(lineage_snapshot_store.list_snapshots(version), [])

            lineage_snapshot_store.clear_failure(version)
            snapshot = lineage_snapshot_store.ensure_snapshot(version)
            self.assertTrue(snapshot["frozen"])
            self.assertEqual(snapshot["node_count"], 2)
            resumed = lineage_snapshot_store.get_write_state(version)
            self.assertEqual(resumed["cursor"], 2)
            self.assertEqual(resumed["status"], "frozen")

            # 恢复成功后再次构建仍是同一个快照
            again = lineage_snapshot_store.ensure_snapshot(version)
            self.assertEqual(again["id"], snapshot["id"])
            self.assertEqual(len(lineage_snapshot_store.list_snapshots(version)), 1)
        finally:
            lineage_snapshot_store.clear_failure(version)
            store.rows("stream_lineage").remove(first_row)
            store.rows("stream_lineage").remove(second_row)


class TestDeadLetterReplayIntegration(unittest.TestCase):
    """死信处理接入快照：重放目标判断保持原口径。"""

    def setUp(self) -> None:
        self.service = DeadLetterService()
        self.snapshot = lineage_snapshot_store.ensure_snapshot("v1")
        self.context = lineage_snapshot_store.deliver_to_dead_letter("v1")

    def test_delivery_pins_current_version(self) -> None:
        self.assertEqual(self.context["snapshot_version"], "v1")
        self.assertEqual(self.context["snapshot_id"], self.snapshot["id"])
        context = self.service.lineage_context()
        self.assertEqual(context["snapshot_revision"], self.snapshot["revision"])

    def test_replay_target_matches_legacy_first_match(self) -> None:
        result = self.service.resolve_replay_target("TOPI-0001")
        self.assertEqual(result["重放目标"], "order_agg_hourly")
        self.assertEqual(result["命中血缘记录"], 1)
        self.assertTrue(result["snapshot_connected"])
        self.assertEqual(result["snapshot_version"], "v1")

    def test_replay_target_unknown_topic(self) -> None:
        result = self.service.resolve_replay_target("TOPI-NOT-EXISTS")
        self.assertIsNone(result["重放目标"])
        self.assertIsNone(result["命中血缘记录"])

    def test_replay_targets_bulk_covers_all_dead_letters(self) -> None:
        targets = self.service.replay_targets()
        self.assertEqual(len(targets), len(store.rows("dead_letter")))
        by_topic = {item["原始主题"]: item["重放目标"] for item in targets}
        # 与重构前 _find_by_field 首条命中的口径一致
        self.assertEqual(by_topic["TOPI-0001"], "order_agg_hourly")
        self.assertEqual(by_topic["TOPI-0002"], "user_funnel_daily")

    def test_replay_judgment_stays_on_delivered_revision(self) -> None:
        # 交付后即使血缘表变化，死信仍按保留的快照版本判断
        store.rows("stream_lineage").append(_extra_lineage_row())
        try:
            result = self.service.resolve_replay_target("TOPI-0001")
            self.assertEqual(result["重放目标"], "order_agg_hourly")
            self.assertEqual(result["snapshot_revision"], self.snapshot["revision"])
        finally:
            store.rows("stream_lineage").pop()


if __name__ == "__main__":
    unittest.main()
