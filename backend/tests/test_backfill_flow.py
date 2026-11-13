"""补数状态同步到调度待办的回归测试。

覆盖：日期范围与执行参数完整保存、状态单向推进、依赖未完成/失败/重试耗尽拦截、
超时后仅从异常区间继续、重复启动幂等、恢复沿用覆盖模式、重跑页与任务页同一水位。
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def act(path: str, body: dict) -> dict:
    return client.post(path, json=body).json()


def batch(code: str) -> dict:
    items = client.get("/api/backfill").json()["items"]
    return next(item for item in items if item["批次编号"] == code)


def task(code: str) -> dict:
    items = client.get("/api/etl").json()["items"]
    return next(item for item in items if item["任务编号"] == code)


def assert_shared(batch_code: str, task_code: str) -> None:
    """重跑页、任务页、异常区间必须显示同一个已完成水位与缺口。"""
    left = batch(batch_code)
    right = task(task_code)
    assert left["已完成水位"] == right["已完成水位"], (left["已完成水位"], right["已完成水位"])
    assert left["异常区间"] == right["异常区间"], (left["异常区间"], right["异常区间"])


def test_create_keeps_full_range_and_parameters() -> None:
    created = act("/api/backfill", {"values": {
        "批次编号": "BACKFILL-TEST",
        "目标表名": "dwd_test",
        "开始日期": "BEGIN-20260901",
        "结束日期": "20260930",
        "并行数量": 6,
        "覆盖模式": "分区覆盖",
        "关联调度任务": "ETL-0001",
    }})
    assert created["ok"], created
    entry = created["entry"]
    assert entry["开始日期"] == "2026-09-01"
    assert entry["结束日期"] == "2026-09-30"
    assert entry["并行数量"] == 6
    assert entry["覆盖模式"] == "分区覆盖"
    assert entry["异常区间"] == "2026-09-01~2026-09-30"

    invalid = act("/api/backfill", {"values": {
        "批次编号": "BAD-1", "目标表名": "x", "开始日期": "2026-09-01",
        "结束日期": "2026-09-02", "并行数量": 0, "覆盖模式": "",
    }})
    assert not invalid["ok"], invalid
    assert "并行数量" in invalid["message"]
    assert "覆盖模式" in invalid["message"]


def test_unfinished_dependency_blocks_running() -> None:
    blocked = act("/api/backfill/2/actions", {"action": "启动重跑"})
    assert blocked["ok"] is False, blocked
    assert "尚未完成" in blocked["message"]
    entry = batch("BACKFILL-0002")
    assert entry["重跑状态"] == "草稿"
    assert entry["未完成依赖"] == "ETL-0002"
    assert entry["调度待办状态"] == "等待依赖"
    assert entry["待处理原因"]
    assert task("ETL-0002")["异常区间"] == entry["异常区间"]

    exhausted = act("/api/backfill/3/actions", {"action": "启动重跑"})
    assert exhausted["ok"] is False
    assert "重试已耗尽" in exhausted["message"]
    reason = batch("BACKFILL-0003")["待处理原因"]
    assert "3/3" in reason


def test_state_progression_timeout_and_resume() -> None:
    assert act("/api/etl/2/actions", {"action": "完成依赖", "已完成水位": "2026-09-30"})["ok"]

    started = act("/api/backfill/2/actions", {"action": "启动重跑"})
    assert started["ok"] and started["entry"]["重跑状态"] == "运行中"
    before = client.get("/api/backfill/runs?batch_code=BACKFILL-0002").json()["total"]
    repeated = act("/api/backfill/2/actions", {"action": "启动重跑"})
    assert repeated["ok"] and "重复启动已忽略" in repeated["message"]
    assert client.get("/api/backfill/runs?batch_code=BACKFILL-0002").json()["total"] == before

    paused = act("/api/backfill/2/actions", {"action": "暂停重跑"})
    assert paused["ok"] and paused["entry"]["重跑状态"] == "已暂停"

    resumed = act("/api/backfill/2/actions", {
        "action": "恢复重跑",
        "执行结果": "超时",
        "超时日期": "2026-09-15",
        "覆盖模式": "全表覆盖",
    })
    assert resumed["ok"], resumed
    entry = resumed["entry"]
    assert entry["覆盖模式"] == "分区覆盖"
    assert entry["已完成水位"] == "2026-09-15"
    assert entry["异常区间"] == "2026-09-16~2026-09-30"
    assert_shared("BACKFILL-0002", "ETL-0002")
    runs = client.get("/api/backfill/runs?batch_code=BACKFILL-0002").json()["items"]
    assert runs[-1]["本次写入区间"] == "2026-09-01~2026-09-15"

    finished = act("/api/backfill/2/actions", {"action": "恢复重跑", "执行结果": "成功"})
    assert finished["ok"], finished
    entry = finished["entry"]
    assert entry["重跑状态"] == "已完成"
    assert entry["已完成水位"] == "2026-09-30"
    assert entry["异常区间"] == ""
    runs = client.get("/api/backfill/runs?batch_code=BACKFILL-0002").json()["items"]
    assert runs[-1]["本次写入区间"] == "2026-09-16~2026-09-30"
    assert_shared("BACKFILL-0002", "ETL-0002")


def test_todo_and_task_share_watermark() -> None:
    todos = {item["批次编号"]: item for item in client.get("/api/backfill/todos").json()["items"]}
    etl_todos = {item["批次编号"]: item for item in client.get("/api/etl/todos").json()["items"]}
    for code in ("BACKFILL-0002", "BACKFILL-0003"):
        assert todos[code]["已完成水位"] == etl_todos[code]["已完成水位"]
        assert todos[code]["异常区间"] == etl_todos[code]["异常区间"]
        assert todos[code]["调度待办状态"] in {"等待依赖", "异常待处理", "运行中", "待启动", "已完成"}
    third = todos["BACKFILL-0003"]
    assert third["未完成依赖"] == "ETL-0003"
    assert "重试已耗尽" in third["待处理原因"]
