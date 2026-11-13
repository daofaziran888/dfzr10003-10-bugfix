"""日期区间工具：补数链路里所有的水位、成功区间、异常区间都按这一套口径解析。

区间统一使用 ISO 日期，展示格式为 `2026-09-10~2026-09-12`，多段之间用英文逗号分隔。
内部计算按“日期集合”进行，避免区间拼接时把已成功的日期重复算进异常区间。
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Iterable

DateRange = tuple[date, date]

_DATE_RE = re.compile(r"(\d{4}[-/.]\d{1,2}[-/.]\d{1,2}|\d{8})")
_PREFIX_RE = re.compile(r"^(?:BEGIN|START|FROM)[-_:\s]*", re.IGNORECASE)
_SPLIT_RE = re.compile(r"[,，;；、\n]+")


def parse_date(value: Any) -> date | None:
    """尽量宽松地解析日期，支持 2026-09-01、20260901、2026/09/01、BEGIN-20260901。"""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    text = _PREFIX_RE.sub("", text)
    text = text.replace("年", "-").replace("月", "-").replace("日", "")
    match = _DATE_RE.search(text)
    if not match:
        return None
    token = match.group(1).replace("/", "-").replace(".", "-")
    if re.fullmatch(r"\d{8}", token.replace("-", "")) and "-" not in token:
        token = f"{token[:4]}-{token[4:6]}-{token[6:]}"
    try:
        return datetime.strptime(token, "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_ranges(value: Any) -> list[DateRange]:
    """把字符串、列表、字典解析成有序且合法的日期区间列表。"""
    if value is None or value == "":
        return []
    if isinstance(value, (list, tuple, set)):
        result: list[DateRange] = []
        for item in value:
            result.extend(parse_ranges(item))
        return merge_ranges(result)
    if isinstance(value, dict):
        start = parse_date(value.get("开始") or value.get("开始日期") or value.get("start"))
        end = parse_date(value.get("结束") or value.get("结束日期") or value.get("end"))
        if start and end:
            return merge_ranges([(min(start, end), max(start, end))])
        return []

    text = str(value).strip()
    if not text:
        return []
    result = []
    for segment in _SPLIT_RE.split(text):
        segment = segment.strip()
        if not segment:
            continue
        segment = segment.replace("年", "-").replace("月", "-").replace("日", "")
        tokens = _DATE_RE.findall(segment)
        parsed = [parse_date(token) for token in tokens]
        parsed = [item for item in parsed if item is not None]
        if not parsed:
            continue
        if len(parsed) == 1:
            result.append((parsed[0], parsed[0]))
            continue
        # 一段里出现两个日期时按区间处理；出现多个日期时，前两个组成区间，其余按单日。
        result.append((min(parsed[0], parsed[1]), max(parsed[0], parsed[1])))
        for item in parsed[2:]:
            result.append((item, item))
    return merge_ranges(result)


def merge_ranges(ranges: Iterable[DateRange]) -> list[DateRange]:
    """合并重叠或首尾相接的区间，保证同一日期只出现一次。"""
    ordered = sorted(ranges, key=lambda item: (item[0], item[1]))
    merged: list[DateRange] = []
    for start, end in ordered:
        if start > end:
            start, end = end, start
        if not merged:
            merged.append((start, end))
            continue
        last_start, last_end = merged[-1]
        if start <= last_end + timedelta(days=1):
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged


def range_dates(ranges: Iterable[DateRange]) -> set[date]:
    result: set[date] = set()
    for start, end in ranges:
        cursor = start
        while cursor <= end:
            result.add(cursor)
            cursor += timedelta(days=1)
    return result


def ranges_from_dates(dates: Iterable[date]) -> list[DateRange]:
    ordered = sorted(set(dates))
    if not ordered:
        return []
    result: list[DateRange] = []
    start = prev = ordered[0]
    for item in ordered[1:]:
        if item == prev + timedelta(days=1):
            prev = item
            continue
        result.append((start, prev))
        start = prev = item
    result.append((start, prev))
    return result


def union_ranges(*groups: Iterable[DateRange]) -> list[DateRange]:
    combined: list[DateRange] = []
    for group in groups:
        combined.extend(group)
    return merge_ranges(combined)


def intersect_ranges(left: Iterable[DateRange], right: Iterable[DateRange]) -> list[DateRange]:
    right_dates = range_dates(right)
    return ranges_from_dates(item for item in range_dates(left) if item in right_dates)


def subtract_ranges(base: Iterable[DateRange], remove: Iterable[DateRange]) -> list[DateRange]:
    remove_dates = range_dates(remove)
    return ranges_from_dates(item for item in range_dates(base) if item not in remove_dates)


def format_ranges(ranges: Iterable[DateRange]) -> str:
    merged = merge_ranges(ranges)
    parts = []
    for start, end in merged:
        if start == end:
            parts.append(start.isoformat())
        else:
            parts.append(f"{start.isoformat()}~{end.isoformat()}")
    return ",".join(parts)


def date_list(ranges: Iterable[DateRange]) -> list[str]:
    return [item.isoformat() for item in sorted(range_dates(ranges))]


def max_date(ranges: Iterable[DateRange]) -> date | None:
    all_dates = range_dates(ranges)
    return max(all_dates) if all_dates else None


def min_date(ranges: Iterable[DateRange]) -> date | None:
    all_dates = range_dates(ranges)
    return min(all_dates) if all_dates else None
