"""ETL 任务调度业务规则：执行记录、依赖推进与调度待办都收在这里。

任务页不再自己拼“未完成依赖/水位”，而是读 `backfill_ledger.task_view`：
- 依赖结果以最近一条已持久化执行记录为准；
- 水位、异常区间直接引用关联重跑批次的同一份待办；
- 未完成依赖必须给出待处理原因，不能用“已完成”掩盖缺口。
"""
from __future__ import annotations

from typing import Any

from app.services import backfill_ledger as ledger
from app.services import ranges
from app.store import store

MODULE = "etl"
REQUIRED_FIELDS = ["任务编号", "任务名称", "依赖任务"]
ACTION_RULES = {
    "启动任务": ledger.RUNNING_STATUS,
    "暂停任务": ledger.RUNNING_STATUS,
    "归档任务": ledger.PAUSED_STATUS,
    "完成依赖": ledger.DONE_STATUS,
    "记录失败": ledger.PAUSED_STATUS,
    "记录超时": ledger.PAUSED_STATUS,
    "记录中断": ledger.PAUSED_STATUS,
}
START_ACTIONS = {"启动任务"}


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


class EtlService:
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
                if keyword in str(row.get("任务编号", "")) or keyword in str(row.get("任务名称", ""))
            ]
        views = [ledger.task_view(row) for row in rows]
        if status:
            views = [view for view in views if view["任务状态"] == status]
        total = len(views)
        start = max(page - 1, 0) * size
        return views[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        ledger.sync_all_todos()
        return ledger.task_view(entry)

    def list_todos(self, *, task_code: str | None = None) -> list[dict[str, Any]]:
        ledger.sync_all_todos()
        rows = store.rows(ledger.TODOS)
        if task_code:
            rows = [row for row in rows if task_code in ledger.split_refs(row.get("关联调度任务"))]
        return [dict(row) for row in rows]

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not _text(values.get(field))]
        if missing:
            return None, [f"缺少必填字段：{'、'.join(missing)}"]
        code = _text(values.get("任务编号"))
        if any(str(row.get("任务编号")) == code for row in store.rows(MODULE)):
            return None, [f"任务编号 {code} 已存在，不能重复登记"]
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({
            "任务编号": code,
            "任务名称": _text(values.get("任务名称")),
            "依赖任务": _text(values.get("依赖任务")),
            "调度表达式": _text(values.get("调度表达式")),
            "重试次数": _as_int(values.get("重试次数")),
            "重试上限": _as_int(values.get("重试上限")) or 3,
            "超时分钟": _as_int(values.get("超时分钟")),
            "告警渠道": _text(values.get("告警渠道")),
            "任务状态": ledger.DRAFT_STATUS,
            "status": ledger.DRAFT_STATUS,
            "最近执行结果": "",
            "已完成水位": "",
            "已完成区间": "",
            "待处理原因": "",
            "pending": True,
            "abnormal": False,
        })
        rows.append(entry)
        return ledger.task_view(entry), []

    def run_action(
        self,
        entry_id: int,
        action: str,
        values: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"调度任务 {entry_id} 不存在或已归档"
        action = _text(action)
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于ETL任务调度可执行范围"
        values = values or {}

        if action in START_ACTIONS:
            result, message = self._start(entry, values)
        elif action == "暂停任务":
            result, message = self._pause(entry)
        elif action == "归档任务":
            result, message = self._archive(entry)
        elif action == "完成依赖":
            result, message = self._finish(entry, values)
        else:
            result, message = self._record_outcome(entry, action, values)

        ledger.sync_all_todos()
        return result, message

    # ------------------------------------------------------------------ 动作
    def _start(self, entry: dict[str, Any], values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        code = entry.get("任务编号")
        status = str(entry.get("status") or ledger.DRAFT_STATUS)
        if status == ledger.RUNNING_STATUS:
            return ledger.task_view(entry), f"任务 {code} 已在运行中，本次重复启动已忽略"
        if status == ledger.DONE_STATUS:
            return ledger.task_view(entry), f"任务 {code} 已完成，本次重复启动已忽略"
        if status == ledger.PAUSED_STATUS and str(entry.get("待处理原因", "")).startswith("任务已归档"):
            return None, f"任务 {code} 已归档，不能再次启动"

        dependency = ledger.dependency_snapshot(entry)
        if not dependency["ok"]:
            entry["待处理原因"] = dependency["待处理原因"]
            return None, f"调度任务 {code} 未进入运行中：{dependency['待处理原因']}"

        self._apply(entry, {
            "任务状态": ledger.RUNNING_STATUS,
            "执行结果": "运行中",
            "待处理原因": "",
            "pending": True,
            "abnormal": False,
        })
        self._append(entry, {
            "触发动作": "启动任务",
            "执行结果": ledger.RUNNING_STATUS,
            "任务状态": ledger.RUNNING_STATUS,
            "本次写入区间": "",
            "已完成区间": str(entry.get("已完成区间") or ""),
            "已完成水位": str(entry.get("已完成水位") or ""),
            "重试次数": _as_int(entry.get("重试次数")),
            "重试上限": _as_int(entry.get("重试上限")),
            "原因": "",
        })
        return ledger.task_view(entry), f"调度任务 {code} 已启动"

    def _pause(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        code = entry.get("任务编号")
        status = str(entry.get("status") or ledger.DRAFT_STATUS)
        if status == ledger.PAUSED_STATUS:
            return ledger.task_view(entry), f"任务 {code} 已处于已暂停，本次暂停已忽略"
        if status == ledger.DRAFT_STATUS:
            return None, f"调度任务 {code} 尚未启动，不能暂停"
        if status == ledger.DONE_STATUS:
            return None, f"调度任务 {code} 已完成，不能暂停"
        reason = "运行被人工暂停，待处理原因保留"
        self._apply(entry, {
            "任务状态": ledger.PAUSED_STATUS,
            "执行结果": "已暂停",
            "待处理原因": reason,
            "pending": True,
            "abnormal": True,
        })
        self._append(entry, {
            "触发动作": "暂停任务",
            "执行结果": "已暂停",
            "任务状态": ledger.PAUSED_STATUS,
            "本次写入区间": "",
            "已完成区间": str(entry.get("已完成区间") or ""),
            "已完成水位": str(entry.get("已完成水位") or ""),
            "重试次数": _as_int(entry.get("重试次数")),
            "重试上限": _as_int(entry.get("重试上限")),
            "原因": reason,
        })
        return ledger.task_view(entry), f"调度任务 {code} 已暂停"

    def _archive(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        code = entry.get("任务编号")
        status = str(entry.get("status") or ledger.DRAFT_STATUS)
        if status == ledger.DONE_STATUS:
            return None, f"调度任务 {code} 已完成，不能归档"
        if status == ledger.PAUSED_STATUS and str(entry.get("待处理原因", "")).startswith("任务已归档"):
            return ledger.task_view(entry), f"任务 {code} 已归档，本次归档已忽略"
        reason = "任务已归档，未完成水位保留在待处理原因中"
        self._apply(entry, {
            "任务状态": ledger.PAUSED_STATUS,
            "执行结果": "已暂停",
            "待处理原因": reason,
            "pending": True,
            "abnormal": True,
        })
        self._append(entry, {
            "触发动作": "归档任务",
            "执行结果": "已暂停",
            "任务状态": ledger.PAUSED_STATUS,
            "本次写入区间": "",
            "已完成区间": str(entry.get("已完成区间") or ""),
            "已完成水位": str(entry.get("已完成水位") or ""),
            "重试次数": _as_int(entry.get("重试次数")),
            "重试上限": _as_int(entry.get("重试上限")),
            "原因": reason,
        })
        return ledger.task_view(entry), f"调度任务 {code} 已归档"

    def _finish(self, entry: dict[str, Any], values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """推进依赖：把本次成功写入的日期并入任务水位，且不能越过上游水位。"""
        code = entry.get("任务编号")
        status = str(entry.get("status") or ledger.DRAFT_STATUS)
        if status == ledger.DRAFT_STATUS:
            return None, f"调度任务 {code} 尚未启动，不能推进依赖"

        dependency = ledger.dependency_snapshot(entry)
        if not dependency["ok"]:
            return None, (
                f"调度任务 {code} 未推进依赖：{dependency['待处理原因']}"
            )

        before = ranges.parse_ranges(
            ledger.task_effective_state(entry).get("已完成区间")
            or entry.get("已完成区间")
        )
        start = ranges.min_date(before) or ranges.parse_date(entry.get("开始日期"))
        watermark_input = ranges.parse_date(values.get("已完成水位") or values.get("推进日期"))
        written = ranges.parse_ranges(values.get("推进区间") or values.get("推进日期"))
        if not written and start and watermark_input:
            written = [(start, watermark_input)]
        if not written and watermark_input:
            written = [(watermark_input, watermark_input)]

        # 上游水位是推进上限；没有依赖任务时不受限制。
        cap_note = ""
        cap = ranges.parse_date(dependency["依赖水位"]) if dependency["引用任务"] else None
        if cap is not None:
            if start is None or cap < start:
                return None, (
                    f"调度任务 {code} 的推进水位 {cap.isoformat()} "
                    f"早于已确认起点，不能回退水位"
                )
            written = ranges.intersect_ranges(written, [(start or cap, cap)])
            if ranges.max_date(written) == cap:
                cap_note = f"，水位受上游限制在 {cap.isoformat()}"

        if not written:
            return None, f"调度任务 {code} 未提供可推进的日期，水位保持 {entry.get('已完成水位') or '无'}"

        after = ranges.union_ranges(before, written)
        watermark = ranges.max_date(after)
        if before and not ranges.subtract_ranges(written, before):
            return ledger.task_view(entry), (
                f"调度任务 {code} 已确认水位 {watermark.isoformat() if watermark else '无'}，"
                "本次推进已写过，未重复写入"
            )

        entry.update({
            "已完成区间": ranges.format_ranges(after),
            "已完成水位": watermark.isoformat() if watermark else "",
            "任务状态": ledger.DONE_STATUS,
            "status": ledger.DONE_STATUS,
            "最近执行结果": "成功",
            "待处理原因": "",
            "pending": False,
            "abnormal": False,
        })
        self._append(entry, {
            "触发动作": "完成依赖",
            "执行结果": "成功",
            "任务状态": ledger.DONE_STATUS,
            "本次写入区间": ranges.format_ranges(written),
            "已完成区间": ranges.format_ranges(after),
            "已完成水位": watermark.isoformat() if watermark else "",
            "重试次数": _as_int(entry.get("重试次数")),
            "重试上限": _as_int(entry.get("重试上限")),
            "原因": f"已确认水位 {watermark.isoformat() if watermark else '无'}{cap_note}",
        })
        return ledger.task_view(entry), (
            f"调度任务 {code} 水位推进到 {watermark.isoformat() if watermark else '无'}{cap_note}"
        )

    def _record_outcome(
        self,
        entry: dict[str, Any],
        action: str,
        values: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str]:
        """记录失败 / 超时 / 中断，重试耗尽后不允许把任务当成已完成。"""
        code = entry.get("任务编号")
        outcome_map = {"记录失败": "失败", "记录超时": "超时", "记录中断": "中断"}
        outcome = outcome_map[action]
        detail = _text(values.get("原因") or values.get("失败原因")) or f"任务{outcome}"
        used = _as_int(entry.get("重试次数"))
        limit = _as_int(entry.get("重试上限"))
        retry_ok = limit <= 0 or used < limit
        if retry_ok:
            used += 1
            reason = f"{detail}；重试 {used}/{limit}" if limit else f"{detail}；已重试 {used} 次"
            self._apply(entry, {
                "任务状态": ledger.PAUSED_STATUS,
                "最近执行结果": outcome,
                "重试次数": used,
                "待处理原因": reason,
                "pending": True,
                "abnormal": True,
            })
            result = outcome
        else:
            reason = f"{detail}；重试已耗尽（{used}/{limit} 次）"
            self._apply(entry, {
                "任务状态": ledger.PAUSED_STATUS,
                "最近执行结果": "重试失败",
                "重试次数": used,
                "待处理原因": reason,
                "pending": True,
                "abnormal": True,
            })
            result = "重试失败"
        self._append(entry, {
            "触发动作": action,
            "执行结果": result,
            "任务状态": ledger.PAUSED_STATUS,
            "本次写入区间": "",
            "已完成区间": str(entry.get("已完成区间") or ""),
            "已完成水位": str(entry.get("已完成水位") or ""),
            "重试次数": used,
            "重试上限": limit,
            "原因": reason,
        })
        return ledger.task_view(entry), f"调度任务 {code} 已记录{result}：{reason}"

    # ------------------------------------------------------------------ 内部
    @staticmethod
    def _apply(entry: dict[str, Any], values: dict[str, Any]) -> None:
        entry.update(values)
        entry["status"] = entry.get("任务状态") or entry.get("status")

    def _append(self, entry: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        record = ledger.append_record(ledger.ETL_RUNS, {
            "任务编号": str(entry.get("任务编号") or ""),
            **payload,
        })
        entry["最近执行时间"] = record["记录时间"]
        entry["最近执行记录"] = record["seq"]
        return record
