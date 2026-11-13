"""ETL任务调度接口：维护调度任务，覆盖启动任务、暂停任务、归档任务与依赖推进。

任务页读取的未完成依赖与水位来自 `/api/backfill/todos` 同一份调度待办。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services import backfill_ledger as ledger
from app.services.etl import EtlService
from app.store import store

router = APIRouter(prefix="/api/etl", tags=["ETL任务调度"])

service = EtlService()

LIST_FIELDS = [
    "任务编号", "任务名称", "依赖任务", "调度表达式", "重试次数", "超时分钟",
    "告警渠道", "任务状态", "已完成水位", "异常区间", "未完成依赖", "待处理原因",
]
STATUSES = ["草稿", "运行中", "已暂停", "已完成"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按任务编号或任务名称检索"),
    status: str | None = Query(default=None, description="草稿、运行中、已暂停、已完成"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按任务编号与状态过滤ETL任务调度列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/todos", response_model=dict)
def list_todos(task_code: str | None = Query(default=None, description="只看某个任务关联的待办")):
    """调度待办：与重跑页共用同一份未完成依赖、水位与异常区间。"""
    items = service.list_todos(task_code=task_code)
    return {"items": items, "total": len(items)}


@router.get("/executions", response_model=dict)
def list_executions(task_code: str | None = Query(default=None, description="只看某个任务的执行记录")):
    """调度执行记录（只追加）：依赖冲突时以序号最大的记录为准。"""
    rows = ledger.records_for(ledger.ETL_RUNS, "任务编号", task_code or "")
    if not task_code:
        rows = [dict(row) for row in store.rows(ledger.ETL_RUNS)]
    rows.sort(key=lambda row: int(row.get("seq", 0)))
    return {"items": rows, "total": len(rows)}


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出ETL任务调度清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "etl", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条调度任务明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"调度任务 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条调度任务，缺字段时说明原因而不是静默丢弃。"""
    entry, problems = service.create_entry(payload.values)
    if problems:
        return ActionResult(ok=False, message="；".join(problems))
    return ActionResult(ok=True, message="调度任务已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """执行启动、暂停、归档，或推进依赖/记录失败；非法动作会被拦下并说明原因。"""
    content = payload.action_values()
    entry, message = service.run_action(entry_id, str(content.get("action") or ""), content)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
