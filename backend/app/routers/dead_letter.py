"""死信处理接口：维护死信记录，覆盖加入处置、标记重放、标记丢弃等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.dead_letter import DeadLetterService

router = APIRouter(prefix="/api/dead_letter", tags=["死信处理"])

service = DeadLetterService()

LIST_FIELDS = ["记录编号", "原始主题", "原始分区", "失败原因", "原始消息体", "重试次数", "入库时间", "处置状态"]
STATUSES = ["草稿", "待处置", "已重放", "已丢弃"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按记录编号检索"),
    status: str | None = Query(default=None, description="草稿、待处置、已重放、已丢弃"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、记录编号关键字与状态过滤死信处理列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="dead_letter.csv"'})


@router.get("/lineage-context")
def get_lineage_context() -> dict:
    """死信处理当前保留的血缘图版本快照上下文（跨模块交付，保留当前版本）。"""
    context = service.lineage_context()
    return {"context": context}


@router.get("/replay-targets")
def list_replay_targets() -> dict:
    """批量核对死信记录的重放目标，判断口径与重构前保持一致。"""
    items = service.replay_targets()
    return {"items": items, "total": len(items), "context": service.lineage_context()}


@router.get("/{entry_id}/replay-target")
def get_replay_target(entry_id: int) -> dict:
    """核对单条死信记录的重放目标（基于冻结血缘快照的同名节点统一口径）。"""
    result = service.replay_target_for_entry(entry_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"死信记录 {entry_id} 不存在或已归档")
    return result


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条死信记录明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"死信记录 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条死信记录，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="死信记录已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条死信记录执行加入处置、标记重放、标记丢弃；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
