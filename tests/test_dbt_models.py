"""dbt models on a small hand-written scenario, so every expected number is known exactly."""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import pytest
from conftest import SALT, make_event

from learning_events.contracts import ContractRegistry
from learning_events.ingest import Ingestor
from learning_events.pipeline import DbtRun, run_dbt
from learning_events.warehouse import Warehouse, connect


def plan(learner: str, ts: str, new_plan: str) -> dict:
    return make_event("plan_changed", learner=learner, ts=ts, object={"plan": new_plan})


def scenario() -> tuple[list[dict], list[dict]]:
    """(on-time events, one late event). Learner A changes plan; learner B goes quiet."""
    a, b = "LA", "LB"
    on_time = [
        plan(a, "2026-03-01T08:00:00Z", "free"),
        plan(a, "2026-03-03T12:00:00Z", "premium"),
        plan(a, "2026-03-04T09:00:00Z", "premium"),  # re-sent webhook: not a new version
        plan(a, "2026-03-10T00:00:00Z", "free"),
        plan(b, "2026-03-01T08:00:00Z", "free"),
        # A, Monday 2 March: gaps of exactly 30:00 (same session) then 30:01 (new session)
        make_event("lesson_started", a, "2026-03-02T10:00:00Z", "a1"),
        make_event("quiz_answered", a, "2026-03-02T10:30:00Z", "a1"),
        make_event("lesson_completed", a, "2026-03-02T11:00:01Z", "a1"),
        # A, Thursday 5 March, while premium
        make_event("lesson_started", a, "2026-03-05T09:00:00Z", "a2"),
        make_event(
            "quiz_answered",
            a,
            "2026-03-05T09:05:00Z",
            "a2",
            object={"lesson_id": "LES-1", "question_id": "Q1"},
        ),
        make_event("lesson_completed", a, "2026-03-05T09:10:00Z", "a2"),
        # A, Thursday 12 March, free again, abandons the lesson
        make_event("lesson_started", a, "2026-03-12T17:00:00Z", "a3"),
        # B: one unfinished lesson on 2 March, then nothing
        make_event("lesson_started", b, "2026-03-02T09:00:00Z", "b1"),
    ]
    late = [
        make_event(
            "quiz_answered",
            a,
            "2026-03-05T09:07:00Z",
            "a2",
            object={"lesson_id": "LES-1", "question_id": "Q2"},
            result={"success": False, "score_scaled": 0.0, "response_ms": 9000},
        )
    ]
    return on_time, late


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[Path, DbtRun]]:
    path = tmp_path_factory.mktemp("dbt") / "scenario.duckdb"
    wh = connect(path)
    wh.init_schema()
    ingestor = Ingestor(wh, ContractRegistry.load(), SALT)
    on_time, late = scenario()
    ingestor.ingest(on_time, datetime(2026, 3, 12, 18))
    ingestor.ingest(late, datetime(2026, 3, 14, 8))  # synced two days later
    wh.close()
    run = run_dbt(str(path), ["build"], target_path=path.parent / "target")
    yield path, run


@pytest.fixture
def wh(built: tuple[Path, DbtRun]) -> Iterator[Warehouse]:
    warehouse = connect(built[0])
    yield warehouse
    warehouse.close()


def test_dbt_build_passes_with_every_test(built: tuple[Path, DbtRun]) -> None:
    run = built[1]
    assert run.ok, run.log_tail
    assert run.count("model", "success") == 10
    assert run.count("test", "pass") >= 35
    assert {r.status for r in run.results} <= {"success", "pass"}


def test_sessionization_gap_of_exactly_30_minutes_stays_in_session(wh: Warehouse) -> None:
    sessions = wh.execute(
        "SELECT CAST(session_start AS VARCHAR), events, duration_seconds "
        "FROM analytics.int_sessions WHERE learner_id = 'LA' ORDER BY session_start"
    )
    assert sessions == [
        ("2026-03-02 10:00:00", 2, 1800),  # 10:00 and 10:30 together
        ("2026-03-02 11:00:01", 1, 0),  # 30:01 later: a new session
        ("2026-03-05 09:00:00", 4, 600),  # includes the late quiz answer at 09:07
        ("2026-03-12 17:00:00", 1, 0),
    ]


def test_scd2_dimension_has_one_row_per_plan_period(wh: Warehouse) -> None:
    rows = wh.execute(
        "SELECT version, plan, CAST(valid_from AS VARCHAR), CAST(valid_to AS VARCHAR), is_current "
        "FROM analytics.dim_learner WHERE learner_id = 'LA' ORDER BY version"
    )
    assert rows == [
        (1, "free", "2026-03-01 08:00:00", "2026-03-03 12:00:00", False),
        (2, "premium", "2026-03-03 12:00:00", "2026-03-10 00:00:00", False),
        (3, "free", "2026-03-10 00:00:00", None, True),
    ]


def test_attempts_get_the_plan_valid_at_their_start(wh: Warehouse) -> None:
    rows = wh.execute(
        "SELECT attempt_id, plan_at_start, is_completed, quiz_answers "
        "FROM analytics.fct_lesson_attempts ORDER BY attempt_id"
    )
    assert rows == [
        ("a1", "free", True, 1),
        ("a2", "premium", True, 2),
        ("a3", "free", False, 0),
        ("b1", "free", False, 0),
    ]


def test_teacher_weekly_counts_active_completion_and_risk(wh: Warehouse) -> None:
    rows = wh.execute(
        "SELECT CAST(week_start AS VARCHAR), enrolled_learners, active_learners, "
        "round(completion_rate, 3), round(avg_quiz_score, 3), learners_at_risk "
        "FROM analytics.mart_teacher_weekly ORDER BY week_start"
    )
    assert rows == [
        ("2026-03-02", 2, 2, 0.667, 0.667, 0),  # a1, a2 done; b1 not. Quiz: 1, 1, 0
        ("2026-03-09", 2, 1, 0.0, None, 1),  # B inactive for the whole week
    ]


def test_singular_test_catches_a_marathon_session(
    built: tuple[Path, DbtRun], tmp_path: Path
) -> None:
    """Events every 29 minutes for 4.8 hours chain into one impossible session."""
    path = tmp_path / "marathon.duckdb"
    shutil.copy(built[0], path)
    wh = connect(path)
    events = [plan("LC", "2026-03-01T08:00:00Z", "free")] + [
        make_event(
            "quiz_answered",
            "LC",
            f"2026-03-03T{8 + (29 * i) // 60:02d}:{(29 * i) % 60:02d}:00Z",
            "c1",
        )
        for i in range(11)
    ]
    Ingestor(wh, ContractRegistry.load(), SALT).ingest(events, datetime(2026, 3, 4))
    wh.close()
    run = run_dbt(str(path), ["build", "--select", "+int_sessions"], target_path=tmp_path / "t")
    failed = [r.unique_id for r in run.results if r.status == "fail"]
    assert not run.ok
    assert failed == ["test.learning_events.assert_no_session_longer_than_4_hours"]
