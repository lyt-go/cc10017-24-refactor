"""订单主题血缘图构建器与版本快照仓库。

重构目标：把原先散落在不同入口的「节点过滤、连边、同名节点查找」收拢到同一处，
保证流式血缘页、全平台血缘图与死信处理使用同一套历史口径；同时为每次构建生成
可回查的血缘图版本快照，支持写入中断后的断点续传，并保证同一血缘版本的重复构建
只会得到同一个快照。

历史口径（重构前后用户可查询的节点与边集合必须一致，不得修改）：
- 节点 id 取血缘记录 id（缺省按行序 i+1）；
- 节点 name 取记录字典的第二个键对应的值（种子数据里即 status 文本），无键时为空串；
- 节点 status 缺省为「草稿」；
- 节点展开除 id/status/pending/abnormal 外的全部业务字段；
- 边按节点行序首尾相连，标注固定为「依赖」。
"""
from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime
from typing import Any, Callable

from app.store import store

MODULE = "stream_lineage"

# 历史口径常量：与旧 StreamLineageService.STATUS_ORDER[0] 保持一致
DEFAULT_NODE_STATUS = "草稿"
DEFAULT_EDGE_LABEL = "依赖"
# 未显式声明链路版本的记录统一归入该版本，保证示例数据仍落在同一历史版本内
DEFAULT_VERSION = "v1"
VERSION_FIELD = "链路版本"

SNAPSHOT_TABLE = "lineage_graph_snapshots"
WRITE_STATE_TABLE = "lineage_graph_write_state"
DEAD_LETTER_CONTEXT_TABLE = "dead_letter_lineage_context"

# 边顺序取舍：保留旧边顺序（旧版边集与新节点索引在同一历史版本相遇时的冻结策略）
EDGE_POLICY_LEGACY_ORDER = "legacy_order"


# ─────────────────────────────────────────────
# 统一构建口径：过滤、节点、连边、同名查找
# ─────────────────────────────────────────────

def record_version(row: dict[str, Any]) -> str:
    """读取血缘记录所属链路版本；缺失时归入默认历史版本。"""
    value = row.get(VERSION_FIELD)
    text = str(value).strip() if value is not None else ""
    return text or DEFAULT_VERSION


def filter_records(
    rows: list[dict[str, Any]] | None = None,
    *,
    version: str | None = None,
    predicate: Callable[[dict[str, Any]], bool] | None = None,
) -> list[dict[str, Any]]:
    """唯一的节点过滤入口：按版本与可选谓词筛选血缘记录。

    不传任何条件时返回全部记录，与重构前 get_graph 的输入完全一致。
    """
    source = store.rows(MODULE) if rows is None else rows
    filtered = source
    if version is not None:
        filtered = [row for row in filtered if record_version(row) == version]
    if predicate is not None:
        filtered = [row for row in filtered if predicate(row)]
    return filtered


def _legacy_node_name(row: dict[str, Any]) -> str:
    """复刻历史 name 口径：取记录字典第二个键的值，没有第二个键时为空串。"""
    keys = list(row.keys())
    name_key = keys[1] if len(keys) > 1 else "name"
    return str(row.get(name_key, ""))


def build_nodes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """按历史口径把血缘记录构造成图节点。"""
    nodes: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        node: dict[str, Any] = {
            "id": int(row.get("id", i + 1)),
            "name": _legacy_node_name(row),
            "status": str(row.get("status", DEFAULT_NODE_STATUS)),
        }
        for key, value in row.items():
            if key not in ("id", "status", "pending", "abnormal"):
                node[key] = value
        nodes.append(node)
    return nodes


def build_edges(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """按历史口径连边：行序首尾相连，标注固定为「依赖」。"""
    edges: list[dict[str, Any]] = []
    for i in range(len(nodes) - 1):
        edges.append({
            "from": nodes[i]["id"],
            "to": nodes[i + 1]["id"],
            "label": DEFAULT_EDGE_LABEL,
        })
    return edges


def build_graph(rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """完整复刻重构前 StreamLineageService.get_graph 的节点与边输出。"""
    source = store.rows(MODULE) if rows is None else rows
    nodes = build_nodes(source)
    edges = build_edges(nodes)
    return {"nodes": nodes, "edges": edges}


def build_version_graph(version: str) -> dict[str, Any]:
    """构建某个血缘版本的图：过滤口径与连边口径都走统一构建器。"""
    rows = filter_records(version=version)
    return build_graph(rows)


def find_node_by_id(graph: dict[str, Any], node_id: Any) -> dict[str, Any] | None:
    """按 id 精确查找节点。"""
    target = int(node_id)
    for node in graph.get("nodes", []):
        if int(node.get("id", 0)) == target:
            return node
    return None


def find_node_by_name(graph: dict[str, Any], name: str | None) -> dict[str, Any] | None:
    """同名节点统一查找口径：返回行序首个匹配（与旧 next(...) 行为一致但只此一处）。"""
    if name is None:
        return None
    for node in graph.get("nodes", []):
        if node.get("name") == name:
            return node
    return None


def duplicate_names(graph: dict[str, Any]) -> list[str]:
    """列出图中重名节点名称，便于快照标注同名节点的收敛口径。"""
    seen: set[str] = set()
    duplicated: set[str] = set()
    for node in graph.get("nodes", []):
        name = str(node.get("name", ""))
        if name in seen:
            duplicated.add(name)
        seen.add(name)
    return sorted(duplicated)


def graph_fingerprint(graph: dict[str, Any]) -> str:
    """节点/边集合的稳定指纹，用于判断重复构建与冻结冲突。"""
    payload = {
        "nodes": [
            {"id": node.get("id"), "name": node.get("name"), "status": node.get("status")}
            for node in graph.get("nodes", [])
        ],
        "edges": [
            {"from": edge.get("from"), "to": edge.get("to"), "label": edge.get("label")}
            for edge in graph.get("edges", [])
        ],
    }
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


# ─────────────────────────────────────────────
# 版本快照仓库：幂等冻结、断点续传、跨模块交付
# ─────────────────────────────────────────────

class LineageSnapshotStore:
    """血缘图版本快照的写入与回查，全部落在内存仓库的附属产物表中。"""

    def __init__(self) -> None:
        # 附属产物表按需创建（store.rows 会 setdefault），避免启动时改变模块清单
        # 测试用故障注入：{version: 在处理完第 N 行后中断}
        self._fail_after: dict[str, int] = {}

    # ── 故障注入（仅用于验证写入中断恢复）──
    def arm_failure(self, version: str, after_cursor: int) -> None:
        self._fail_after[version] = after_cursor

    def clear_failure(self, version: str) -> None:
        self._fail_after.pop(version, None)

    # ── 读取 ──
    def list_snapshots(self, version: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(SNAPSHOT_TABLE)
        if version is not None:
            rows = [row for row in rows if row.get("version") == version]
        return [copy.deepcopy(row) for row in rows]

    def get_snapshot(self, version: str, revision: int | None = None) -> dict[str, Any] | None:
        rows = [row for row in store.rows(SNAPSHOT_TABLE) if row.get("version") == version]
        if not rows:
            return None
        target = rows[-1] if revision is None else next(
            (row for row in rows if int(row.get("revision", 0)) == revision), None)
        return copy.deepcopy(target) if target is not None else None

    def current_snapshot(self) -> dict[str, Any] | None:
        """最近冻结的快照即当前版本；未冻结任何快照时返回 None。"""
        rows = [row for row in store.rows(SNAPSHOT_TABLE) if row.get("frozen")]
        if not rows:
            return None
        return copy.deepcopy(max(rows, key=lambda row: str(row.get("frozen_at", ""))))

    def get_write_state(self, version: str) -> dict[str, Any] | None:
        for row in store.rows(WRITE_STATE_TABLE):
            if row.get("version") == version:
                return copy.deepcopy(row)
        return None

    # ── 写入链路：同版本幂等 + 断点续传 ──
    def ensure_snapshot(self, version: str) -> dict[str, Any]:
        """确保某血缘版本存在冻结快照；重复构建同一版本只返回同一个快照。

        1. 已有同指纹冻结快照：直接返回，不重复写入；
        2. 存在中断/进行中的写入状态：从最后成功位置续传；
        3. 否则从零开始按行写入，每行推进游标并持久化，便于中断恢复。
        """
        existing = self._latest_frozen(version)
        rows = filter_records(version=version)
        fresh_graph = build_graph(rows)
        if existing is not None and existing.get("fingerprint") == graph_fingerprint(fresh_graph):
            return copy.deepcopy(existing)

        state = self._load_or_init_state(version, len(rows))
        nodes = self._resume_nodes(rows, state)
        self._write_rows(version, rows, nodes, state)
        edges = build_edges(nodes)
        graph = {"nodes": nodes, "edges": edges}

        # 最后成功位置由「重构前后节点过滤与连边结果同一历史口径」确定
        verify_graph = build_version_graph(version)
        if graph_fingerprint(graph) != graph_fingerprint(verify_graph):
            raise RuntimeError(f"血缘版本 {version} 续传结果与历史口径校验不一致，已拒绝冻结")
        return self._freeze(version, graph, rows, state)

    def _write_rows(
        self,
        version: str,
        rows: list[dict[str, Any]],
        nodes: list[dict[str, Any]],
        state: dict[str, Any],
    ) -> None:
        """按游标逐行处理节点，并在故障注入点模拟写入中断。"""
        cursor = int(state.get("cursor", 0))
        while cursor < len(rows):
            node = build_nodes([rows[cursor]])[0]
            nodes.append(node)
            cursor += 1
            state["cursor"] = cursor
            state["updated_at"] = datetime.now().isoformat()
            fail_after = self._fail_after.get(version)
            if fail_after is not None and cursor >= fail_after and cursor < len(rows):
                state["status"] = "interrupted"
                state["fail_at"] = datetime.now().isoformat()
                self._persist_state(version, state)
                raise RuntimeError(f"血缘版本 {version} 快照写入在第 {cursor} 行后中断")
        state["status"] = "done"
        state["updated_at"] = datetime.now().isoformat()
        self._persist_state(version, state)

    def _resume_nodes(
        self, rows: list[dict[str, Any]], state: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """从最后成功游标恢复已处理节点，保证续传不重复写入。"""
        cursor = int(state.get("cursor", 0))
        return build_nodes(rows[:cursor])

    def _freeze(
        self,
        version: str,
        graph: dict[str, Any],
        rows: list[dict[str, Any]],
        state: dict[str, Any],
    ) -> dict[str, Any]:
        """冻结快照；同版本出现不同图（历史冲突）时升修订号而不是覆盖旧快照。"""
        fingerprint = graph_fingerprint(graph)
        previous = self._latest_frozen(version)
        now = datetime.now().isoformat()
        if previous is not None and previous.get("fingerprint") == fingerprint:
            return copy.deepcopy(previous)
        revision = int(previous.get("revision", 0)) + 1 if previous is not None else 1
        snapshot = {
            "id": len(store.rows(SNAPSHOT_TABLE)) + 1,
            "version": version,
            "revision": revision,
            "edge_policy": EDGE_POLICY_LEGACY_ORDER,
            "frozen": True,
            "node_count": len(graph["nodes"]),
            "edge_count": len(graph["edges"]),
            "duplicate_names": duplicate_names(graph),
            "fingerprint": fingerprint,
            "nodes": copy.deepcopy(graph["nodes"]),
            "edges": copy.deepcopy(graph["edges"]),
            "source_record_ids": [int(row.get("id", i + 1)) for i, row in enumerate(rows)],
            "created_at": now,
            "frozen_at": now,
            "write_cursor": state.get("cursor"),
        }
        store.rows(SNAPSHOT_TABLE).append(snapshot)
        state["status"] = "frozen"
        state["snapshot_id"] = snapshot["id"]
        state["updated_at"] = now
        self._persist_state(version, state)
        return copy.deepcopy(snapshot)

    # ── 重构前后对比 ──
    def compare_with_live(self, version: str) -> dict[str, Any]:
        """比较冻结快照（重构前口径）与当前实时构建（重构后口径）的节点与边集合。"""
        snapshot = self.get_snapshot(version)
        live = build_version_graph(version)
        snapshot_nodes = snapshot.get("nodes", []) if snapshot else []
        snapshot_edges = snapshot.get("edges", []) if snapshot else []
        node_ids_before = {int(n.get("id", 0)) for n in snapshot_nodes}
        node_ids_after = {int(n.get("id", 0)) for n in live.get("nodes", [])}
        edge_keys_before = {(e.get("from"), e.get("to"), e.get("label")) for e in snapshot_edges}
        edge_keys_after = {
            (e.get("from"), e.get("to"), e.get("label")) for e in live.get("edges", [])
        }
        same_order = [
            (e.get("from"), e.get("to"), e.get("label")) for e in snapshot_edges
        ] == [
            (e.get("from"), e.get("to"), e.get("label")) for e in live.get("edges", [])
        ]
        return {
            "version": version,
            "snapshot_revision": snapshot.get("revision") if snapshot else None,
            "nodes_equal": node_ids_before == node_ids_after,
            "edges_equal": edge_keys_before == edge_keys_after,
            "edge_order_preserved": same_order,
            "node_count_before": len(snapshot_nodes),
            "node_count_after": len(live.get("nodes", [])),
            "edge_count_before": len(snapshot_edges),
            "edge_count_after": len(live.get("edges", [])),
            "nodes_only_in_snapshot": sorted(node_ids_before - node_ids_after),
            "nodes_only_in_live": sorted(node_ids_after - node_ids_before),
            "edges_only_in_snapshot": sorted(edge_keys_before - edge_keys_after),
            "edges_only_in_live": sorted(edge_keys_after - edge_keys_before),
        }

    # ── 跨模块交付：流式血缘 → 死信处理 ──
    def deliver_to_dead_letter(self, version: str | None = None) -> dict[str, Any]:
        """把当前血缘图版本快照交付给死信处理，并保留当前版本不被后续构建覆盖。"""
        target_version = version or (self.current_snapshot() or {}).get("version") or DEFAULT_VERSION
        snapshot = self.ensure_snapshot(target_version)
        context_rows = store.rows(DEAD_LETTER_CONTEXT_TABLE)
        context = next((row for row in context_rows if row.get("scope") == "dead_letter"), None)
        delivered_at = datetime.now().isoformat()
        if context is None:
            context = {
                "scope": "dead_letter",
                "snapshot_version": snapshot["version"],
                "snapshot_revision": snapshot["revision"],
                "snapshot_id": snapshot["id"],
                "delivered_at": delivered_at,
            }
            context_rows.append(context)
        else:
            context["snapshot_version"] = snapshot["version"]
            context["snapshot_revision"] = snapshot["revision"]
            context["snapshot_id"] = snapshot["id"]
            context["delivered_at"] = delivered_at
        return copy.deepcopy(context)

    def dead_letter_context(self) -> dict[str, Any] | None:
        """死信处理当前保留的血缘版本；未交付过时回退到当前快照。"""
        context_rows = store.rows(DEAD_LETTER_CONTEXT_TABLE)
        context = next((row for row in context_rows if row.get("scope") == "dead_letter"), None)
        if context is not None:
            return copy.deepcopy(context)
        current = self.current_snapshot()
        if current is None:
            return None
        return {
            "scope": "dead_letter",
            "snapshot_version": current["version"],
            "snapshot_revision": current["revision"],
            "snapshot_id": current["id"],
            "delivered_at": None,
        }

    def snapshot_for_dead_letter(self) -> dict[str, Any] | None:
        """死信处理重放判断使用的冻结快照（保留当前版本）。"""
        context = self.dead_letter_context()
        if context is None:
            return None
        return self.get_snapshot(
            str(context["snapshot_version"]), int(context["snapshot_revision"])
        )

    # ── 内部辅助 ──
    def _latest_frozen(self, version: str) -> dict[str, Any] | None:
        rows = [
            row for row in store.rows(SNAPSHOT_TABLE)
            if row.get("version") == version and row.get("frozen")
        ]
        return rows[-1] if rows else None

    def _load_or_init_state(self, version: str, total: int) -> dict[str, Any]:
        for row in store.rows(WRITE_STATE_TABLE):
            if row.get("version") == version:
                if row.get("status") in ("in_progress", "interrupted"):
                    row["total"] = total
                    return row
                # 已完成/已冻结且走到这里说明数据发生变化，开启新一轮写入
                row["status"] = "in_progress"
                row["cursor"] = 0
                row["total"] = total
                row["updated_at"] = datetime.now().isoformat()
                return row
        state = {
            "version": version,
            "status": "in_progress",
            "cursor": 0,
            "total": total,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }
        store.rows(WRITE_STATE_TABLE).append(state)
        return state

    def _persist_state(self, version: str, state: dict[str, Any]) -> None:
        for row in store.rows(WRITE_STATE_TABLE):
            if row.get("version") == version:
                row.update(state)
                return
        store.rows(WRITE_STATE_TABLE).append(dict(state))


lineage_snapshot_store = LineageSnapshotStore()
