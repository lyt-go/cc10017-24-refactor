"""流式血缘 DAG 拓扑接口：返回节点、连线图结构。"""
from __future__ import annotations

import csv
import io
import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.stream_lineage import StreamLineageService

router = APIRouter(prefix="/api/stream_lineage", tags=["流式血缘"])
service = StreamLineageService()
LIST_FIELDS = ["记录编号", "上游主题", "下游表名", "转换节点", "字段映射", "采集时间", "延迟偏移", "链路版本"]
STATUSES = ["草稿", "待校验", "已生效", "已停用"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def get_graph() -> dict[str, Any]:
    """返回节点与边组成的 DAG 图数据。"""
    return service.get_graph()


@router.get("/snapshots")
def list_snapshots(version: str | None = Query(default=None)) -> dict[str, Any]:
    """列出血缘图版本快照（可按链路版本过滤），用于回查历史口径。"""
    items = service.list_snapshots(version)
    return {"items": items, "total": len(items), "current": service.current_snapshot()}


@router.post("/snapshots/freeze")
def freeze_snapshot(version: str | None = Query(default=None)) -> ActionResult:
    """冻结血缘图版本快照；同一血缘版本重复构建只会得到同一个快照。"""
    snapshot = service.ensure_snapshot(version)
    return ActionResult(ok=True, message=f"血缘版本 {snapshot['version']} 快照已冻结", entry=snapshot)


@router.get("/snapshots/{version}/compare")
def compare_snapshot(version: str) -> dict[str, Any]:
    """流式血缘页比较重构前后图：节点与边集合必须保持同一历史口径。"""
    snapshot = service.get_snapshot(version)
    if snapshot is None:
        snapshot = service.ensure_snapshot(version)
    return service.compare_snapshot(version)


@router.post("/snapshots/{version}/deliver")
def deliver_snapshot(version: str) -> ActionResult:
    """跨模块把血缘图版本快照从流式血缘交付给死信处理，并保留当前版本。"""
    context = service.deliver_snapshot_to_dead_letter(version)
    return ActionResult(ok=True, message="血缘图版本快照已交付死信处理", entry=context)


@router.get("/snapshots/{version}/write-state")
def get_write_state(version: str) -> dict[str, Any]:
    """回查快照写入链路的最后成功位置，验证写入中断后的恢复进度。"""
    state = service.write_state(version)
    if state is None:
        raise HTTPException(status_code=404, detail=f"血缘版本 {version} 暂无快照写入记录")
    return state


@router.get("/export")
def export_entries() -> StreamingResponse:
    nodes, edges = service.raw_data()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["type", "id", "name", "from", "to", "label", "status"])
    for node in nodes:
        writer.writerow(["node", node.get("id",""), csv_cell(node.get("name","")), "", "", "", node.get("status","")])
    for edge in edges:
        writer.writerow(["edge", "", "", edge.get("from",""), edge.get("to",""), edge.get("label",""), ""])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=stream_lineage.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"血缘记录 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="血缘记录已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
