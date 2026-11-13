"""历史重跑接口：维护重跑批次，覆盖启动重跑、暂停重跑、归档批次等动作。

重跑状态会同步进调度待办 `/api/backfill/todos`，ETL 任务页读取的是同一份待办。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services import backfill_ledger as ledger
from app.services.backfill import BackfillService
from app.store import store

router = APIRouter(prefix="/api/backfill", tags=["历史重跑"])

service = BackfillService()

LIST_FIELDS = [
    "批次编号", "目标表名", "开始日期", "结束日期", "并行数量", "覆盖模式",
    "重跑状态", "已完成水位", "异常区间", "未完成依赖", "待处理原因",
]
STATUSES = ["草稿", "运行中", "已暂停", "已完成"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按批次编号或目标表名检索"),
    status: str | None = Query(default=None, description="草稿、运行中、已暂停、已完成"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按批次编号与状态过滤历史重跑列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/todos", response_model=dict)
def list_todos(batch_code: str | None = Query(default=None, description="只看某个批次的待办")):
    """调度待办：重跑状态、已完成水位、异常区间与待处理原因都在这里。"""
    items = service.list_todos(batch_code=batch_code)
    return {"items": items, "total": len(items)}


@router.get("/runs", response_model=dict)
def list_runs(batch_code: str | None = Query(default=None, description="只看某个批次的执行记录")):
    """补数执行记录（只追加）：每次启动 / 恢复 / 中断都留痕，按序号排序。"""
    rows = ledger.records_for(ledger.BACKFILL_RUNS, "批次编号", batch_code or "")
    if not batch_code:
        rows = [dict(row) for row in store.rows(ledger.BACKFILL_RUNS)]
    rows.sort(key=lambda row: int(row.get("seq", 0)))
    return {"items": rows, "total": len(rows)}


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出历史重跑清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "backfill", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条重跑批次明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"重跑批次 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条重跑批次：日期范围、覆盖模式、并行数量必须完整保存。"""
    entry, problems = service.create_entry(payload.values)
    if problems:
        return ActionResult(ok=False, message="；".join(problems))
    return ActionResult(ok=True, message="重跑批次已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条重跑批次执行动作；依赖未完成或状态不合法时拦下并说明原因。"""
    content = payload.action_values()
    entry, message = service.run_action(entry_id, str(content.get("action") or ""), content)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
