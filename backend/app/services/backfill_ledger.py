"""补数与调度的共享账本。

历史重跑与 ETL 调度之间只有一份事实来源：重跑批次每推进一步，就把状态、已完成水位、
异常区间和待处理原因写进调度待办；任务页读取的是同一份待办，不在列表里另存一份快照。

执行记录只追加、不改写，每条记录带全局递增的 `seq`。依赖结果出现冲突时按以下口径裁决：
- 依赖是否完成：取该任务 `seq` 最大的一条已持久化执行记录，任务行上的旧字段只在没有记录时兜底；
- 依赖已确认的水位：优先取最近记录自带的 `已完成水位`，记录没带水位时回退到最近一条成功记录；
- 重跑批次可见的成功区间：批次最近一条已持久化执行记录里的累计区间，再与依赖已确认水位求交；
- 重跑页、任务页与异常区间全部调用 `shared_view` / `task_view`，保证同一个水位、同一段缺口。
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable

from app.services import ranges
from app.store import store

BACKFILL_RUNS = "backfill_runs"
ETL_RUNS = "etl_executions"
TODOS = "scheduling_todos"

SUCCESS_RESULTS = {"成功", "部分成功", "已完成"}
FAILURE_RESULTS = {"失败", "重试失败"}
INTERRUPT_RESULTS = {"超时", "中断"}
RUNNING_RESULTS = {"已启动", "恢复", "运行中", "等待依赖"}
DONE_STATUS = "已完成"
PAUSED_STATUS = "已暂停"
RUNNING_STATUS = "运行中"
DRAFT_STATUS = "草稿"


def now_text() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")


def append_record(module: str, payload: dict[str, Any]) -> dict[str, Any]:
    """追加一条带全局序号的持久化执行记录。"""
    rows = store.rows(module)
    record = {
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "seq": store.next_sequence(),
        "记录时间": payload.pop("记录时间", None) or now_text(),
    }
    record.update(payload)
    rows.append(record)
    return record


def records_for(module: str, key_field: str, key: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in store.rows(module)
        if str(row.get(key_field)) == key
    ]


def latest_record(module: str, key_field: str, key: str) -> dict[str, Any] | None:
    matches = records_for(module, key_field, key)
    if not matches:
        return None
    return max(matches, key=lambda row: int(row.get("seq", 0)))


def latest_success_record(module: str, key_field: str, key: str) -> dict[str, Any] | None:
    matches = [
        row for row in records_for(module, key_field, key)
        if str(row.get("执行结果")) in SUCCESS_RESULTS
    ]
    if not matches:
        return None
    return max(matches, key=lambda row: int(row.get("seq", 0)))


def latest_batch_run(batch_code: str) -> dict[str, Any] | None:
    return latest_record(BACKFILL_RUNS, "批次编号", batch_code)


def latest_etl_record(task_code: str) -> dict[str, Any] | None:
    return latest_record(ETL_RUNS, "任务编号", task_code)


def split_refs(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, (list, tuple, set)):
        tokens = [str(item).strip() for item in value]
    else:
        tokens = [token.strip() for token in ranges._SPLIT_RE.split(str(value))]
    return [
        token for token in tokens
        if token and token not in {"无", "无依赖", "none", "None", "-", "N/A"}
    ]


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def find_task(task_code: str) -> dict[str, Any] | None:
    code = str(task_code or "").strip()
    if not code:
        return None
    for row in store.rows("etl"):
        if str(row.get("任务编号")) == code or str(row.get("id")) == code:
            return row
    return None


def task_effective_state(task: dict[str, Any]) -> dict[str, Any]:
    """以最近一条已持久化执行记录为准，汇总任务的执行结果、状态与水位。"""
    code = str(task.get("任务编号") or "")
    latest = latest_etl_record(code)
    fallback_success = latest_success_record(ETL_RUNS, "任务编号", code)
    if latest is None:
        return {
            "任务编号": code,
            "执行结果": str(task.get("最近执行结果") or ""),
            "任务状态": str(task.get("任务状态") or task.get("status") or DRAFT_STATUS),
            "已完成水位": str(task.get("已完成水位") or ""),
            "已完成区间": str(task.get("已完成区间") or ""),
            "重试次数": _as_int(task.get("重试次数")),
            "重试上限": _as_int(task.get("重试上限")),
            "待处理原因": str(task.get("待处理原因") or ""),
            "最近记录序号": 0,
        }
    watermark = str(latest.get("已完成水位") or "")
    completed_ranges = str(latest.get("已完成区间") or "")
    if fallback_success is not None:
        watermark = watermark or str(fallback_success.get("已完成水位") or "")
        completed_ranges = completed_ranges or str(fallback_success.get("已完成区间") or "")
    return {
        "任务编号": code,
        "执行结果": str(latest.get("执行结果") or ""),
        "任务状态": str(latest.get("任务状态") or latest.get("执行结果") or ""),
        "已完成水位": watermark,
        "已完成区间": completed_ranges,
        "重试次数": _as_int(latest.get("重试次数")) or _as_int(task.get("重试次数")),
        "重试上限": _as_int(latest.get("重试上限")) or _as_int(task.get("重试上限")),
        "待处理原因": str(latest.get("原因") or task.get("待处理原因") or ""),
        "最近记录序号": int(latest.get("seq", 0)),
    }


def batch_range(entry: dict[str, Any]) -> list[ranges.DateRange]:
    """批次登记的整体日期范围：开始日期到结束日期。"""
    start = ranges.parse_date(entry.get("开始日期"))
    end = ranges.parse_date(entry.get("结束日期")) or start
    if start is None or end is None:
        return []
    return ranges.merge_ranges([(min(start, end), max(start, end))])


def batch_scope(entry: dict[str, Any]) -> list[ranges.DateRange]:
    """本次补数要处理的日期。

    异常区间只有在登记时显式给出、且与整体范围不同（确实是局部补数）时才算处理范围；
    登记的是完整范围时，它只是“尚未完成的日期”，不能因此把已成功区间排除在外。
    """
    full = batch_range(entry)
    explicit = ranges.intersect_ranges(
        ranges.parse_ranges(entry.get("异常区间") or entry.get("异常范围")),
        full,
    )
    if explicit and ranges.subtract_ranges(full, explicit):
        return explicit
    return full


def batch_persisted_success(entry: dict[str, Any]) -> list[ranges.DateRange]:
    """批次最近一条已持久化执行记录写下的累计成功区间。

    执行记录只追加，所以“最近一次记录”就是最终裁决；记录里没写区间时
    回退到最近一条成功记录，仍然没有才用批次行上的字段兜底。
    """
    full = batch_range(entry)
    code = str(entry.get("批次编号") or "")
    latest = latest_batch_run(code)
    source = latest.get("已完成区间") if latest else None
    if not source and latest:
        success = latest_success_record(BACKFILL_RUNS, "批次编号", code)
        source = success.get("已完成区间") if success else None
    if not source:
        source = entry.get("已完成区间")
    return ranges.intersect_ranges(ranges.parse_ranges(source), full)


def _dependency_confirmed_ranges(entry: dict[str, Any], dependency: dict[str, Any]) -> list[ranges.DateRange] | None:
    """依赖已确认的日期；没有依赖任务时返回 None。"""
    if not dependency["引用任务"]:
        return None
    watermark = ranges.parse_date(dependency.get("依赖水位"))
    if watermark is None:
        return [] if not dependency["ok"] else None
    return [(date.min, watermark)]


def batch_effective_success(
    entry: dict[str, Any],
    dependency: dict[str, Any] | None = None,
) -> list[ranges.DateRange]:
    """对外可见的成功区间：已持久化写入且被依赖的最新记录确认。"""
    dependency = dependency or dependency_snapshot(entry)
    persisted = batch_persisted_success(entry)
    confirmed = _dependency_confirmed_ranges(entry, dependency)
    if confirmed is None:
        return persisted
    return ranges.intersect_ranges(persisted, confirmed)


def batch_remaining(entry: dict[str, Any], dependency: dict[str, Any] | None = None) -> list[ranges.DateRange]:
    """还没被确认成功的日期；恢复与重试都只处理这一段。"""
    return ranges.subtract_ranges(batch_scope(entry), batch_effective_success(entry, dependency))


def batch_watermark(entry: dict[str, Any], dependency: dict[str, Any] | None = None) -> str:
    value = ranges.max_date(batch_effective_success(entry, dependency))
    return value.isoformat() if value else ""


def dependency_snapshot(entry: dict[str, Any]) -> dict[str, Any]:
    """检查重跑批次依赖的 ETL 任务；未完成、失败或重试耗尽都给出明确原因。"""
    refs = split_refs(entry.get("关联调度任务") or entry.get("依赖任务"))
    base = {
        "ok": True,
        "未完成依赖": "",
        "待处理原因": "",
        "依赖水位": "",
        "任务状态": "",
        "重试信息": "",
        "引用任务": [],
        "依赖已完成区间": "",
    }
    if not refs:
        return base

    unfinished: list[str] = []
    reasons: list[str] = []
    watermarks: list[str] = []
    states: list[str] = []
    retries: list[str] = []
    confirmed_ranges: list[ranges.DateRange] = []
    refs_info: list[dict[str, Any]] = []

    for code in refs:
        task = find_task(code)
        if task is None:
            unfinished.append(code)
            reasons.append(f"依赖任务 {code} 不存在，无法确认是否完成")
            refs_info.append({"任务编号": code, "存在": False, "完成": False})
            continue
        state = task_effective_state(task)
        result = state["执行结果"]
        task_status = state["任务状态"]
        used = state["重试次数"]
        limit = state["重试上限"]
        done = task_status == DONE_STATUS and result in SUCCESS_RESULTS
        reason = ""
        if done:
            watermarks.append(state["已完成水位"] or "")
            states.append(result)
            refs_info.append({"任务编号": code, **state, "完成": True})
            continue

        unfinished.append(code)
        if used > 0 and limit > 0 and used >= limit:
            reason = f"依赖任务 {code} 重试已耗尽（已重试 {used}/{limit} 次）"
        elif result in FAILURE_RESULTS:
            reason = (
                f"依赖任务 {code} 最近一次已持久化执行记录为失败："
                f"{state['待处理原因'] or '未提供失败原因'}"
            )
        elif result in INTERRUPT_RESULTS:
            reason = (
                f"依赖任务 {code} 最近一次已持久化执行记录为{result}："
                f"{state['待处理原因'] or '未提供中断原因'}"
            )
        elif task_status == DONE_STATUS or result in SUCCESS_RESULTS:
            reason = (
                f"依赖任务 {code} 的执行结果与完成状态冲突，"
                "按最近一次已持久化执行记录判定为未完成"
            )
        else:
            reason = (
                f"依赖任务 {code} 尚未完成（最近一次已持久化执行记录："
                f"{task_status or result or '未开始'}）"
            )
        reasons.append(reason)
        states.append(result or task_status)
        retries.append(f"{used}/{limit}" if limit else str(used))
        watermarks.append(state["已完成水位"] or "")
        refs_info.append({"任务编号": code, **state, "完成": False, "原因": reason})

    # 多个依赖时水位取最小，避免把最乐观的进度当成整体水位。
    valid_watermarks = [item for item in watermarks if item]
    watermark = min(valid_watermarks) if valid_watermarks else ""
    if watermark:
        cap = ranges.parse_date(watermark)
        confirmed_ranges = [(date.min, cap)]
    return {
        "ok": not unfinished,
        "未完成依赖": ",".join(unfinished),
        "待处理原因": "；".join(reasons),
        "依赖水位": watermark,
        "任务状态": ",".join(states),
        "重试信息": ",".join(retries),
        "引用任务": refs_info,
        "依赖已完成区间": ranges.format_ranges(confirmed_ranges),
    }


def tasks_for_batch(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """批次关联的调度任务；任务记录的写入方是任务自己，批次只读取。"""
    tasks = []
    for code in split_refs(entry.get("关联调度任务") or entry.get("依赖任务")):
        task = find_task(code)
        if task is not None:
            tasks.append(task)
    return tasks


def batch_view(entry: dict[str, Any]) -> dict[str, Any]:
    """重跑批次的最终展示口径：响应与调度待办都来自同一份计算。

    水位与异常区间取“批次执行记录 + 依赖水位”的交集；这里不再单独维护
    一套独立于执行记录的状态，避免重跑页与任务页出现两个水位。
    """
    dependency = dependency_snapshot(entry)
    effective_success = batch_effective_success(entry, dependency)
    remaining = batch_remaining(entry, dependency)
    latest = latest_batch_run(str(entry.get("批次编号") or ""))
    status = str(entry.get("status") or entry.get("重跑状态") or DRAFT_STATUS)
    reason = str(entry.get("待处理原因") or "")
    if latest is not None and latest.get("原因"):
        reason = str(latest.get("原因") or reason)

    if status == DONE_STATUS and remaining:
        status = PAUSED_STATUS
        reason = reason or (
            f"批次曾被标记完成，但异常区间 {ranges.format_ranges(remaining)} 仍未确认，已回退为已暂停"
        )
    if status == DONE_STATUS:
        reason = ""
    elif not dependency["ok"] and dependency["待处理原因"] not in reason:
        reason = f"{reason}；{dependency['待处理原因']}" if reason else dependency["待处理原因"]
    elif not reason:
        reason = "待启动，尚未产生执行记录" if status == DRAFT_STATUS else "等待下一次推进"

    return {
        "id": entry.get("id"),
        "批次编号": str(entry.get("批次编号") or ""),
        "目标表名": str(entry.get("目标表名") or ""),
        "开始日期": _date_text(entry.get("开始日期")),
        "结束日期": _date_text(entry.get("结束日期")),
        "并行数量": entry.get("并行数量"),
        "覆盖模式": str(entry.get("覆盖模式") or ""),
        "关联调度任务": str(entry.get("关联调度任务") or ""),
        "重跑状态": status,
        "status": status,
        "已完成区间": ranges.format_ranges(effective_success),
        "已完成水位": ranges.max_date(effective_success) and ranges.max_date(effective_success).isoformat() or "",
        "异常区间": ranges.format_ranges(remaining),
        "待执行区间": ranges.format_ranges(remaining),
        "未完成依赖": dependency["未完成依赖"],
        "待处理原因": reason,
        "调度待办状态": _todo_status(status, dependency["ok"], remaining),
        "pending": status != DONE_STATUS,
        "abnormal": bool(remaining),
        "最近执行记录": int(latest.get("seq", 0)) if latest else 0,
        "最近执行时间": str(latest.get("记录时间") or "") if latest else "",
    }


def status_from_record(value: str) -> str:
    """执行记录里的状态文本映射为批次状态；识别不了返回空串。"""
    text = str(value or "").strip()
    if text in {RUNNING_STATUS, "运行中", "运行"}:
        return RUNNING_STATUS
    if text in {PAUSED_STATUS, "已暂停", "异常待处理", "等待依赖", "部分成功"}:
        return PAUSED_STATUS
    if text in {DONE_STATUS, "成功", "已完成"}:
        return DONE_STATUS
    if text in {DRAFT_STATUS, "待启动", "已启动"}:
        return DRAFT_STATUS
    return ""


def task_status_from_record(value: str, remaining: Iterable[Any] = ()) -> str:
    """任务执行记录到任务状态的映射；仍有缺口时不能显示已完成。"""
    text = str(value or "").strip()
    has_remaining = bool(list(remaining))
    if text in {DONE_STATUS, "成功", "已完成"}:
        return PAUSED_STATUS if has_remaining else DONE_STATUS
    if text in {RUNNING_STATUS, "运行中", "运行"}:
        return RUNNING_STATUS
    if text in {DRAFT_STATUS, "待启动"}:
        return DRAFT_STATUS
    if text:
        return PAUSED_STATUS
    return ""


def shared_view(entry: dict[str, Any]) -> dict[str, Any]:
    """重跑页与任务页共用的展示口径（`batch_view` 的兼容别名）。"""
    return batch_view(entry)


def _date_text(value: Any) -> str:
    parsed = ranges.parse_date(value)
    return parsed.isoformat() if parsed else str(value or "")


def _todo_status(status: str, dependency_ok: bool, remaining: Iterable[ranges.DateRange]) -> str:
    has_remaining = bool(list(remaining))
    if status == DONE_STATUS and not has_remaining and dependency_ok:
        return DONE_STATUS
    if status == PAUSED_STATUS and not has_remaining and dependency_ok:
        return DONE_STATUS
    if not dependency_ok and status != RUNNING_STATUS:
        return "等待依赖"
    if status == RUNNING_STATUS:
        return "运行中"
    if status == PAUSED_STATUS:
        return "异常待处理" if has_remaining else "已暂停"
    return "待启动"


def sync_all_todos() -> list[dict[str, Any]]:
    """把每个重跑批次的当前状态同步进调度待办。"""
    todos = []
    for entry in store.rows("backfill"):
        todos.append(upsert_todo(entry))
    return todos


def upsert_todo(entry: dict[str, Any], **extra: Any) -> dict[str, Any]:
    view = shared_view(entry)
    view.update(extra)
    rows = store.rows(TODOS)
    todo = next(
        (row for row in rows if str(row.get("批次编号")) == view["批次编号"]),
        None,
    )
    if todo is None:
        todo = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "批次编号": view["批次编号"],
        }
        rows.append(todo)
    todo.update({
        "关联调度任务": view["关联调度任务"],
        "目标表名": view["目标表名"],
        "开始日期": view["开始日期"],
        "结束日期": view["结束日期"],
        "覆盖模式": view["覆盖模式"],
        "并行数量": view["并行数量"],
        "已完成区间": view["已完成区间"],
        "已完成水位": view["已完成水位"],
        "异常区间": view["异常区间"],
        "未完成依赖": view["未完成依赖"],
        "待处理原因": view["待处理原因"],
        "调度待办状态": view["调度待办状态"],
        "重跑状态": view["重跑状态"],
        "pending": view["pending"],
        "abnormal": view["abnormal"],
        "最近执行记录": view["最近执行记录"],
        "最近执行时间": view["最近执行时间"],
        "更新时间": now_text(),
    })
    todo.update(extra)
    return todo


def todo_for_batch(batch_code: str) -> dict[str, Any] | None:
    for row in store.rows(TODOS):
        if str(row.get("批次编号")) == batch_code:
            return row
    return None


def batches_for_task(task_code: str) -> list[dict[str, Any]]:
    code = str(task_code or "")
    if not code:
        return []
    return [
        row for row in store.rows("backfill")
        if code in split_refs(row.get("关联调度任务") or row.get("依赖任务"))
    ]


def task_view(task: dict[str, Any]) -> dict[str, Any]:
    """任务页展示口径：执行结果取最近记录，水位/缺口引用关联批次同一份待办。"""
    state = task_effective_state(task)
    code = str(task.get("任务编号") or "")
    dependency = dependency_snapshot(task)

    batch_views = [shared_view(batch) for batch in batches_for_task(code)]
    # 只取仍然未完成批次的缺口原因，避免把已解决的旧原因一直挂在任务页。
    batch_reasons = [
        item["待处理原因"] for item in batch_views
        if item["待处理原因"] and item["重跑状态"] != DONE_STATUS
    ]
    batch_anomalies = [item["异常区间"] for item in batch_views if item["异常区间"]]
    batch_unfinished = [item["未完成依赖"] for item in batch_views if item["未完成依赖"]]
    batch_todo_states = [item["调度待办状态"] for item in batch_views if item["调度待办状态"]]

    # 任务页与重跑页必须显示同一个水位：有关联批次时完全以批次口径为准，
    # 多个批次取最小；批次尚未产生写入时水位为空，不能用任务自己的水位补位。
    if batch_views:
        watermark = min(item["已完成水位"] for item in batch_views)
    else:
        watermark = state["已完成水位"]
    unfinished = ",".join(dict.fromkeys(batch_unfinished)) or dependency["未完成依赖"]
    reasons = list(dict.fromkeys(batch_reasons))
    if not reasons and not dependency["ok"]:
        reasons = [dependency["待处理原因"]]
    if batch_views and not watermark and state["已完成水位"]:
        reasons.append(
            f"任务自身已持久化水位 {state['已完成水位']}，"
            f"关联批次 {','.join(item['批次编号'] for item in batch_views)} 尚未写入该区间"
        )
    if state["待处理原因"] and state["待处理原因"] not in reasons:
        reasons.append(state["待处理原因"])

    anomaly = ",".join(dict.fromkeys(batch_anomalies))
    # 任务状态取它自己最近一次已持久化执行记录；关联批次仍处于已暂停且有缺口时
    # 不能继续显示已完成，否则会把批次缺口掩盖掉。
    task_status = task_status_from_record(
        state["执行结果"] or state["任务状态"],
    ) or task_status_from_record(state["任务状态"]) or (
        state["任务状态"] if state["任务状态"] else "待启动"
    )
    if task_status == DONE_STATUS and any(
        item["重跑状态"] == PAUSED_STATUS and item["异常区间"] for item in batch_views
    ):
        task_status = PAUSED_STATUS
        if not reasons:
            reasons = ["关联重跑批次仍有未完成区间，任务水位只到已完成水位"]
    todo_states = list(dict.fromkeys(batch_todo_states)) or [task_status]

    pending = bool(reasons) or not dependency["ok"] or bool(anomaly)
    return {
        "id": task.get("id"),
        "任务编号": code,
        "任务名称": str(task.get("任务名称") or ""),
        "依赖任务": str(task.get("依赖任务") or ""),
        "调度表达式": str(task.get("调度表达式") or ""),
        "重试次数": state["重试次数"],
        "重试上限": state["重试上限"],
        "超时分钟": _as_int(task.get("超时分钟")),
        "告警渠道": str(task.get("告警渠道") or ""),
        "任务状态": task_status,
        "status": task_status,
        "最近执行结果": state["执行结果"],
        "已完成水位": watermark,
        "已完成区间": state["已完成区间"],
        "异常区间": anomaly,
        "关联重跑批次": ",".join(item["批次编号"] for item in batch_views),
        "未完成依赖": unfinished,
        "待处理原因": "；".join(reasons),
        "调度待办状态": ",".join(todo_states),
        "pending": pending,
        "abnormal": bool(batch_anomalies) or state["执行结果"] in FAILURE_RESULTS | INTERRUPT_RESULTS,
        "最近执行记录": state["最近记录序号"],
    }
