"""死信处理业务规则：状态流转、字段校验与筛选口径都收在这里。

重放目标判断接入流式血缘交付过来的血缘图版本快照：
判断口径保持原样（按原始主题匹配首条血缘记录的下游表名），
只是数据来源从分散的直接查表改为统一冻结快照，避免同名节点分叉。
"""
from __future__ import annotations

from typing import Any

from app.lineage_graph import lineage_snapshot_store
from app.store import store

MODULE = "dead_letter"
LIST_FIELDS = ["记录编号", "原始主题", "原始分区", "失败原因", "原始消息体", "重试次数", "入库时间", "处置状态"]
REQUIRED_FIELDS = ["记录编号", "原始主题", "原始分区"]
STATUS_ORDER = ["草稿", "待处置", "已重放", "已丢弃"]
ACTION_RULES = {"加入处置": "待处置", "标记重放": "已重放", "标记丢弃": "已丢弃"}
NEGATIVE_ACTIONS = []


class DeadLetterService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("记录编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"死信记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于死信处理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"死信记录已{action}"

    # ── 血缘图版本快照接入 ──
    def lineage_context(self) -> dict[str, Any] | None:
        """死信处理当前保留的血缘版本（由流式血缘跨模块交付）。"""
        return lineage_snapshot_store.dead_letter_context()

    def _lineage_nodes(self) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        """返回重放判断使用的血缘节点集与对应快照；未交付快照时回退实时表。"""
        snapshot = lineage_snapshot_store.snapshot_for_dead_letter()
        if snapshot is not None:
            return list(snapshot.get("nodes", [])), snapshot
        return list(store.rows("stream_lineage")), None

    def resolve_replay_target(self, topic: str | None) -> dict[str, Any]:
        """按原始主题判断重放目标，保持首条匹配的历史口径。

        重构前等价于 pipeline_engine._find_by_field("stream_lineage", "上游主题", topic)
        命中的首条记录；数据源切换为冻结快照后判断结果不变。
        """
        nodes, snapshot = self._lineage_nodes()
        matched = next((node for node in nodes if str(node.get("上游主题", "")) == str(topic)), None)
        return {
            "原始主题": topic,
            "重放目标": matched.get("下游表名") if matched else None,
            "命中血缘记录": matched.get("id") if matched else None,
            "转换节点": matched.get("转换节点") if matched else None,
            "snapshot_version": snapshot.get("version") if snapshot else None,
            "snapshot_revision": snapshot.get("revision") if snapshot else None,
            "snapshot_connected": snapshot is not None,
        }

    def replay_target_for_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        result = self.resolve_replay_target(entry.get("原始主题"))
        result["dead_letter_id"] = entry_id
        result["记录编号"] = entry.get("记录编号")
        return result

    def replay_targets(self) -> list[dict[str, Any]]:
        """列表页批量核对重放目标，逐条沿用同一历史口径。"""
        results = []
        for entry in store.rows(MODULE):
            result = self.resolve_replay_target(entry.get("原始主题"))
            result["dead_letter_id"] = entry.get("id")
            result["记录编号"] = entry.get("记录编号")
            results.append(result)
        return results
