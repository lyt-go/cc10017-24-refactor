"""流式血缘 DAG 图服务：生成节点、边示例结构与流转规则。

重构后节点过滤、连边与同名节点查找统一收口到 app.lineage_graph，
本服务只保留登记/动作等业务流转；get_graph/raw_data 的节点与边输出保持历史口径不变。
"""
from __future__ import annotations

from typing import Any

from app.lineage_graph import (
    DEFAULT_VERSION,
    build_graph,
    build_version_graph,
    lineage_snapshot_store,
)
from app.store import store

MODULE = "stream_lineage"
REQUIRED_FIELDS = ["记录编号", "上游主题", "下游表名"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用记录": "已停用"}
NEGATIVE_ACTIONS = ["停用记录"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


class StreamLineageService:
    def get_graph(self) -> dict[str, Any]:
        """返回与重构前逐字段一致的节点与边（统一构建器口径）。"""
        return build_graph()

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id",0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"血缘记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流式血缘可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"血缘记录已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]

    # ── 血缘图版本快照 ──
    def ensure_snapshot(self, version: str | None = None) -> dict[str, Any]:
        """冻结并返回指定版本（缺省为默认历史版本）的血缘图快照。"""
        return lineage_snapshot_store.ensure_snapshot(version or DEFAULT_VERSION)

    def list_snapshots(self, version: str | None = None) -> list[dict[str, Any]]:
        return lineage_snapshot_store.list_snapshots(version)

    def get_snapshot(self, version: str, revision: int | None = None) -> dict[str, Any] | None:
        return lineage_snapshot_store.get_snapshot(version, revision)

    def current_snapshot(self) -> dict[str, Any] | None:
        return lineage_snapshot_store.current_snapshot()

    def write_state(self, version: str) -> dict[str, Any] | None:
        return lineage_snapshot_store.get_write_state(version)

    def compare_snapshot(self, version: str) -> dict[str, Any]:
        """流式血缘页比较重构前后图：任一历史版本节点与边集合必须相同。"""
        return lineage_snapshot_store.compare_with_live(version)

    def deliver_snapshot_to_dead_letter(self, version: str | None = None) -> dict[str, Any]:
        """跨模块把血缘图版本快照从流式血缘交付给死信处理，保留当前版本。"""
        return lineage_snapshot_store.deliver_to_dead_letter(version)
