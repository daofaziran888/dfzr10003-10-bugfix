"""历史重跑业务规则：状态流转、字段校验与筛选口径都收在这里。

补数链路把“重跑批次 -> 调度待办 -> ETL 任务”当成同一条链路：
- 批次登记时完整保存日期范围、覆盖模式、并行数量，失败时逐项说明原因；
- 状态只允许沿 草稿 -> 运行中 -> 已暂停 -> 已完成 推进：草稿只能启动，
  运行中可以暂停/中断/完成，已暂停只能恢复、归档或直接完成，已完成不再回退；
- 重复提交启动是幂等的，不会追加执行记录；
- 每次启动 / 恢复 / 中断 / 完成都追加一条执行记录，已成功区间不会重复写入；
- 依赖检查统一走 `backfill_ledger.dependency_snapshot`，任务页读的是同一份待办。
"""
from __future__ import annotations

from typing import Any

from app.services import backfill_ledger as ledger
from app.services import ranges
from app.store import store

MODULE = "backfill"
REQUIRED_FIELDS = ["批次编号", "目标表名", "开始日期", "结束日期", "覆盖模式", "并行数量"]
START_ACTIONS = {"启动重跑", "恢复重跑"}
ACTION_RULES = {
    "启动重跑": "运行中",
    "恢复重跑": "运行中",
    "暂停重跑": "已暂停",
    "归档批次": "已暂停",
    "完成重跑": "已完成",
    "记录中断": "已暂停",
    "记录超时": "已暂停",
    "记录失败": "已暂停",
}
# 允许的状态迁移：目标状态必须来自这条边，不能跳级、不能回退到草稿。
TRANSITIONS = {
    "草稿": {"运行中", "已暂停"},
    "运行中": {"已暂停", "已完成"},
    "已暂停": {"运行中", "已完成", "已暂停"},
    "已完成": {"已完成"},
}
INTERRUPT_RESULTS = {"超时", "中断"}
OUTCOME_ACTIONS = {
    "完成重跑": "成功",
    "记录中断": "中断",
    "记录超时": "超时",
    "记录失败": "失败",
}
# 执行结果对应的目标状态；部分成功也先落到已完成，实际是否完成由剩余区间裁决。
OUTCOME_TARGETS = {
    "成功": "已完成",
    "已完成": "已完成",
    "部分成功": "已完成",
    "失败": "已暂停",
    "重试失败": "已暂停",
    "超时": "已暂停",
    "中断": "已暂停",
}


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _as_positive_int(value: Any) -> int | None:
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _can_transition(current: str, target: str) -> bool:
    return target in TRANSITIONS.get(current, {current})


class BackfillService:
    # ------------------------------------------------------------------ 查询
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        ledger.sync_all_todos()
        rows = store.rows(MODULE)
        if keyword:
            rows = [
                row for row in rows
                if keyword in str(row.get("批次编号", ""))
                or keyword in str(row.get("目标表名", ""))
            ]
        if status:
            rows = [row for row in rows if ledger.shared_view(row)["重跑状态"] == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        page_rows = rows[start:start + size]
        return [ledger.shared_view(row) for row in page_rows], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        ledger.upsert_todo(entry)
        return ledger.shared_view(entry)

    def list_todos(self, *, batch_code: str | None = None) -> list[dict[str, Any]]:
        ledger.sync_all_todos()
        rows = store.rows(ledger.TODOS)
        if batch_code:
            rows = [row for row in rows if str(row.get("批次编号")) == batch_code]
        return [dict(row) for row in rows]

    def list_runs(self, *, batch_code: str | None = None) -> list[dict[str, Any]]:
        rows = ledger.records_for(ledger.BACKFILL_RUNS, "批次编号", batch_code or "")
        if not batch_code:
            rows = [dict(row) for row in store.rows(ledger.BACKFILL_RUNS)]
        return sorted(rows, key=lambda row: int(row.get("seq", 0)))

    # ------------------------------------------------------------------ 登记
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记重跑批次：逐项校验并把所有问题一次说清楚，不静默丢字段。"""
        problems: list[str] = []
        missing = [field for field in REQUIRED_FIELDS if not _text(values.get(field))]
        if missing:
            problems.append(f"缺少必填字段：{'、'.join(missing)}")

        batch_code = _text(values.get("批次编号"))
        if batch_code and any(str(row.get("批次编号")) == batch_code for row in store.rows(MODULE)):
            problems.append(f"批次编号 {batch_code} 已存在，不能重复登记")

        start = ranges.parse_date(values.get("开始日期"))
        end = ranges.parse_date(values.get("结束日期"))
        if _text(values.get("开始日期")) and start is None:
            problems.append(f"开始日期「{values.get('开始日期')}」无法解析为有效日期")
        if _text(values.get("结束日期")) and end is None:
            problems.append(f"结束日期「{values.get('结束日期')}」无法解析为有效日期")
        if start and end and end < start:
            problems.append("结束日期不能早于开始日期")

        parallel_text = _text(values.get("并行数量"))
        parallel = _as_positive_int(values.get("并行数量"))
        if parallel_text and parallel is None:
            problems.append(f"并行数量「{parallel_text}」必须是正整数")

        cover_mode = _text(values.get("覆盖模式"))
        if _text(values.get("覆盖模式")) and not cover_mode:
            problems.append("覆盖模式不能为空")

        if problems:
            return None, problems

        full_range = [(start, end)]
        explicit_abnormal = ranges.intersect_ranges(
            ranges.parse_ranges(values.get("异常区间")),
            full_range,
        )

        rows = store.rows(MODULE)
        entry = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "批次编号": batch_code,
            "目标表名": _text(values.get("目标表名")),
            "开始日期": start.isoformat(),
            "结束日期": end.isoformat(),
            "并行数量": parallel,
            "覆盖模式": cover_mode,
            "关联调度任务": _text(values.get("关联调度任务") or values.get("依赖任务")),
            "异常区间": ranges.format_ranges(explicit_abnormal or full_range),
            "已完成区间": "",
            "已完成水位": "",
            "重跑状态": ledger.DRAFT_STATUS,
            "status": ledger.DRAFT_STATUS,
            "待处理原因": "",
            "pending": True,
            "abnormal": False,
        }
        rows.append(entry)
        ledger.upsert_todo(entry)
        return ledger.shared_view(entry), []

    # ------------------------------------------------------------------ 动作
    def run_action(
        self,
        entry_id: int,
        action: str,
        values: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"重跑批次 {entry_id} 不存在或已归档"
        action = _text(action)
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于历史重跑可执行范围"
        values = dict(values or {})

        result: tuple[dict[str, Any] | None, str]
        if action in START_ACTIONS:
            result = self._start(entry, resume=action == "恢复重跑", values=values)
        elif action == "暂停重跑":
            result = self._pause(entry)
        elif action == "归档批次":
            result = self._archive(entry)
        else:
            result = self._finish(entry, OUTCOME_ACTIONS[action], values)

        ledger.upsert_todo(entry)
        return result

    def _record(self, entry: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        record = ledger.append_record(ledger.BACKFILL_RUNS, {
            "批次编号": str(entry.get("批次编号") or ""),
            **payload,
        })
        entry["最近执行记录"] = record["seq"]
        entry["最近执行时间"] = record["记录时间"]
        return record

    def _guard_dependency(
        self,
        entry: dict[str, Any],
        *,
        action: str,
    ) -> tuple[dict[str, Any] | None, str] | None:
        dependency = ledger.dependency_snapshot(entry)
        if dependency["ok"]:
            entry["未完成依赖"] = ""
            return None
        entry["待处理原因"] = dependency["待处理原因"]
        entry["未完成依赖"] = dependency["未完成依赖"]
        ledger.upsert_todo(entry)
        return None, (
            f"重跑批次 {entry.get('批次编号')} 的「{action}」被拒绝："
            f"{dependency['待处理原因']}"
        )

    def _start(
        self,
        entry: dict[str, Any],
        *,
        resume: bool,
        values: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str]:
        batch_code = entry.get("批次编号")
        status = str(entry.get("status") or ledger.DRAFT_STATUS)
        requested = _text(values.get("执行结果"))
        one_shot = bool(requested) and requested not in {
            ledger.RUNNING_STATUS, "已启动", "恢复",
        }

        # 幂等：不带执行结果的重复启动不追加执行记录；带结果时按结果继续推进。
        if status == ledger.RUNNING_STATUS:
            if one_shot:
                return self._finish(entry, requested, values)
            return ledger.shared_view(entry), f"批次 {batch_code} 已在运行中，本次重复启动已忽略"
        if status == ledger.DONE_STATUS:
            return ledger.shared_view(entry), f"批次 {batch_code} 已完成，本次重复启动已忽略"
        if not _can_transition(status, ledger.RUNNING_STATUS):
            return None, f"重跑批次 {batch_code} 当前为{status}，不能启动"

        blocked = self._guard_dependency(entry, action="启动重跑")
        if blocked is not None:
            return blocked

        remaining = ledger.batch_remaining(entry)
        if not remaining:
            entry["status"] = entry["重跑状态"] = ledger.DONE_STATUS
            entry["pending"] = False
            entry["abnormal"] = False
            entry["待处理原因"] = ""
            entry["未完成依赖"] = ""
            return ledger.shared_view(entry), f"重跑批次 {batch_code} 无需重跑，已完成"

        # 覆盖模式属于登记参数，暂停后恢复沿用原值，不接受临时改写。
        cover_note = ""
        requested_cover = _text(values.get("覆盖模式"))
        if requested_cover and requested_cover != _text(entry.get("覆盖模式")):
            cover_note = f"；覆盖模式沿用登记值「{entry.get('覆盖模式')}」"

        triggering = "恢复重跑" if resume or status == ledger.PAUSED_STATUS else "启动重跑"
        self._record(entry, {
            "触发动作": triggering,
            "执行结果": ledger.RUNNING_STATUS,
            "本次写入区间": "",
            "已完成区间": str(entry.get("已完成区间") or ""),
            "已完成水位": str(entry.get("已完成水位") or ""),
            "异常区间": ranges.format_ranges(remaining),
            "覆盖模式": entry.get("覆盖模式") or "",
            "并行数量": entry.get("并行数量"),
            "依赖水位": ledger.dependency_snapshot(entry)["依赖水位"],
            "原因": "已进入运行中，等待执行结果",
        })
        entry["status"] = entry["重跑状态"] = ledger.RUNNING_STATUS
        entry["待处理原因"] = "运行中，等待执行结果"
        entry["pending"] = True
        entry["abnormal"] = bool(remaining)

        # 兼容一次性提交执行结果：启动后同一调用内记录中断/失败/完成。
        if one_shot:
            result, message = self._finish(entry, requested, values)
            if result is None:
                return result, message
            return result, f"{message}{cover_note}"

        return ledger.shared_view(entry), (
            f"重跑批次 {batch_code} 已{triggering.replace('重跑', '')}"
            f"（{ledger.RUNNING_STATUS}），水位 {ledger.shared_view(entry)['已完成水位'] or '无'}"
            f"；异常区间 {ranges.format_ranges(remaining)}{cover_note}"
        )

    def _finish(
        self,
        entry: dict[str, Any],
        outcome: str,
        values: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str]:
        batch_code = entry.get("批次编号")
        status = str(entry.get("status") or ledger.DRAFT_STATUS)
        if status == ledger.DONE_STATUS:
            return ledger.shared_view(entry), f"批次 {batch_code} 已完成，本次执行结果已忽略"
        if status == ledger.DRAFT_STATUS:
            return None, f"重跑批次 {batch_code} 尚未启动，不能记录执行结果"
        if not _can_transition(status, OUTCOME_TARGETS.get(outcome, ledger.PAUSED_STATUS)):
            return None, f"重跑批次 {batch_code} 当前为{status}，不能记录{outcome}"

        blocked = self._guard_dependency(entry, action=f"记录{outcome}")
        if blocked is not None:
            return blocked

        remaining = ledger.batch_remaining(entry)
        if not remaining:
            entry["status"] = entry["重跑状态"] = ledger.DONE_STATUS
            entry["pending"] = False
            entry["abnormal"] = False
            entry["待处理原因"] = ""
            return ledger.shared_view(entry), f"重跑批次 {batch_code} 无需重跑，已完成"

        cutoff = ranges.parse_date(
            values.get("中断日期") or values.get("超时日期") or values.get("执行至")
        )
        if outcome in ledger.SUCCESS_RESULTS and cutoff is None:
            written = remaining
        elif cutoff is not None:
            written = ranges.intersect_ranges(
                remaining, [(ranges.min_date(remaining), cutoff)]
            )
        else:
            written = []

        # 依赖水位是写入上限：依赖只推进到某天时，批次不能越过它宣称成功。
        dependency = ledger.dependency_snapshot(entry)
        dependency_cap = ""
        cap_date = ranges.parse_date(dependency["依赖水位"])
        if cap_date is not None:
            batch_end = ranges.parse_date(entry.get("结束日期"))
            if batch_end and cap_date < batch_end:
                written = ranges.intersect_ranges(
                    written, [(ranges.min_date(remaining), cap_date)]
                )
                dependency_cap = cap_date.isoformat()

        if outcome in ledger.SUCCESS_RESULTS and written != remaining:
            outcome = "部分成功"
        if not written and outcome in ledger.SUCCESS_RESULTS:
            outcome = "部分成功"

        success_before = ledger.batch_persisted_success(entry)
        success_after = ranges.union_ranges(success_before, written)
        remaining_after = ranges.subtract_ranges(ledger.batch_scope(entry), success_after)
        done = not remaining_after and outcome in ledger.SUCCESS_RESULTS

        if done:
            new_status = ledger.DONE_STATUS
            reason = ""
        else:
            new_status = ledger.PAUSED_STATUS
            reason = self._reason_for(outcome, written, remaining_after, values, dependency_cap)

        self._record(entry, {
            "触发动作": "完成重跑" if outcome in ledger.SUCCESS_RESULTS else f"记录{outcome}",
            "执行结果": "成功" if done else outcome,
            "本次写入区间": ranges.format_ranges(written),
            "已完成区间": ranges.format_ranges(success_after),
            "已完成水位": self._watermark(success_after),
            "异常区间": ranges.format_ranges(remaining_after),
            "覆盖模式": entry.get("覆盖模式") or "",
            "并行数量": entry.get("并行数量"),
            "依赖水位": dependency["依赖水位"],
            "原因": reason,
        })

        entry["已完成区间"] = ranges.format_ranges(success_after)
        entry["已完成水位"] = self._watermark(success_after)
        entry["异常区间"] = ranges.format_ranges(remaining_after)
        entry["重跑状态"] = entry["status"] = new_status
        entry["待处理原因"] = reason
        entry["未完成依赖"] = dependency["未完成依赖"]
        entry["pending"] = new_status != ledger.DONE_STATUS
        entry["abnormal"] = new_status == ledger.PAUSED_STATUS and bool(remaining_after)

        if done:
            message = f"重跑批次 {batch_code} 已完成，水位 {entry['已完成水位']}"
        else:
            message = f"重跑批次 {batch_code} 已暂停：{reason}"
        return ledger.shared_view(entry), message

    def _pause(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        batch_code = entry.get("批次编号")
        status = str(entry.get("status") or ledger.DRAFT_STATUS)
        if status == ledger.PAUSED_STATUS:
            return ledger.shared_view(entry), f"批次 {batch_code} 已处于已暂停，本次暂停已忽略"
        if status == ledger.DRAFT_STATUS:
            return None, f"重跑批次 {batch_code} 尚未启动，不能暂停"
        if status == ledger.DONE_STATUS:
            return None, f"重跑批次 {batch_code} 已完成，不能暂停"
        if not ledger.batch_remaining(entry):
            entry["status"] = entry["重跑状态"] = ledger.DONE_STATUS
            entry["pending"] = False
            entry["abnormal"] = False
            entry["待处理原因"] = ""
            return ledger.shared_view(entry), f"重跑批次 {batch_code} 已完成，无需暂停"

        reason = "运行被人工暂停，下次仅从异常区间继续"
        self._record(entry, {
            "触发动作": "暂停重跑",
            "执行结果": "已暂停",
            "本次写入区间": "",
            "已完成区间": str(entry.get("已完成区间") or ""),
            "已完成水位": str(entry.get("已完成水位") or ""),
            "异常区间": ranges.format_ranges(ledger.batch_remaining(entry)),
            "覆盖模式": entry.get("覆盖模式") or "",
            "并行数量": entry.get("并行数量"),
            "原因": reason,
        })
        entry["status"] = entry["重跑状态"] = ledger.PAUSED_STATUS
        entry["待处理原因"] = reason
        entry["pending"] = True
        entry["abnormal"] = bool(ledger.batch_remaining(entry))
        return ledger.shared_view(entry), f"重跑批次 {batch_code} 已暂停，异常区间已保留"

    def _archive(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        batch_code = entry.get("批次编号")
        status = str(entry.get("status") or ledger.DRAFT_STATUS)
        if status == ledger.DONE_STATUS:
            return None, f"重跑批次 {batch_code} 已完成，不能归档"
        if status == ledger.DRAFT_STATUS:
            return None, f"重跑批次 {batch_code} 尚未启动，不能归档"
        if str(entry.get("待处理原因", "")).startswith("批次已归档"):
            return ledger.shared_view(entry), f"批次 {batch_code} 已归档，本次归档已忽略"
        reason = str(entry.get("待处理原因") or "")
        reason = f"批次已归档；{reason}" if reason and not reason.startswith("批次已归档") else (
            reason or "批次已归档，未完成日期保留在异常区间"
        )
        self._record(entry, {
            "触发动作": "归档批次",
            "执行结果": "已暂停",
            "本次写入区间": "",
            "已完成区间": str(entry.get("已完成区间") or ""),
            "已完成水位": str(entry.get("已完成水位") or ""),
            "异常区间": ranges.format_ranges(ledger.batch_remaining(entry)),
            "覆盖模式": entry.get("覆盖模式") or "",
            "并行数量": entry.get("并行数量"),
            "原因": reason,
        })
        entry["status"] = entry["重跑状态"] = ledger.PAUSED_STATUS
        entry["待处理原因"] = reason
        entry["pending"] = bool(ledger.batch_remaining(entry))
        entry["abnormal"] = bool(ledger.batch_remaining(entry))
        return ledger.shared_view(entry), f"重跑批次 {batch_code} 已归档"

    # ------------------------------------------------------------------ 内部
    @staticmethod
    def _watermark(success: list[ranges.DateRange]) -> str:
        value = ranges.max_date(success)
        return value.isoformat() if value else ""

    @staticmethod
    def _reason_for(
        outcome: str,
        written: list[ranges.DateRange],
        remaining_after: list[ranges.DateRange],
        values: dict[str, Any],
        dependency_cap: str,
    ) -> str:
        detail = _text(values.get("原因") or values.get("失败原因"))
        written_text = ranges.format_ranges(written) or "无"
        remaining_text = ranges.format_ranges(remaining_after) or "无"
        if outcome in INTERRUPT_RESULTS:
            text = (
                f"运行{outcome}：本次写入 {written_text}，剩余异常区间 {remaining_text}，"
                "下次仅从异常区间继续"
            )
        elif outcome in ledger.FAILURE_RESULTS:
            text = (
                f"执行失败：{detail or '未提供失败原因'}；本次写入 {written_text}，"
                f"剩余异常区间 {remaining_text}"
            )
        elif outcome == "部分成功":
            text = (
                f"部分成功：{detail or '未覆盖全部日期'}；本次写入 {written_text}，"
                f"剩余异常区间 {remaining_text}"
            )
        else:
            text = f"本次只写入 {written_text}，剩余异常区间 {remaining_text}"
        if dependency_cap:
            text += f"；依赖任务水位只到 {dependency_cap}，批次未越过依赖"
        return text
