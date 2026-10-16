from __future__ import annotations

import uuid
import warnings
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

with warnings.catch_warnings():
    warnings.simplefilter("ignore")  # starlette's httpx deprecation notice, not our code
    from fastapi.testclient import TestClient

from learning_events.api import create_app
from learning_events.contracts import ContractRegistry
from learning_events.ingest import Ingestor
from learning_events.warehouse import Warehouse, connect

SALT = "test-salt-not-secret"


@pytest.fixture(autouse=True)
def _salt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LEVENTS_PII_SALT", SALT)


@pytest.fixture(scope="session")
def registry() -> ContractRegistry:
    return ContractRegistry.load()


@pytest.fixture
def warehouse(tmp_path: Path) -> Iterator[Warehouse]:
    wh = connect(tmp_path / "wh.duckdb")
    wh.init_schema()
    yield wh
    wh.close()


@pytest.fixture
def ingestor(warehouse: Warehouse, registry: ContractRegistry) -> Ingestor:
    return Ingestor(warehouse, registry, SALT)


@pytest.fixture
def client(ingestor: Ingestor) -> Iterator[TestClient]:
    with TestClient(create_app(ingestor, clock=lambda: datetime(2026, 3, 20, 12))) as c:
        yield c


def make_event(
    kind: str,
    learner: str = "L9001",
    ts: str = "2026-03-02T10:00:00Z",
    attempt: str = "a1",
    class_id: str = "T1",
    **overrides: Any,
) -> dict[str, Any]:
    """A valid event of any registered type; `overrides` replace top-level keys."""
    lesson = {"lesson_id": "LES-1", "course_id": "ENG-T1"}
    body: dict[str, Any] = {
        "lesson_started": {"verb": "started", "object": lesson},
        "lesson_completed": {
            "verb": "completed",
            "object": lesson,
            "result": {"duration_seconds": 600, "success": True},
        },
        "quiz_answered": {
            "verb": "answered",
            "object": {"lesson_id": "LES-1", "question_id": "Q1"},
            "result": {"success": True, "score_scaled": 1.0, "response_ms": 4000},
        },
        "video_watched": {
            "verb": "watched",
            "object": {"video_id": "V1", "lesson_id": "LES-1"},
            "result": {"seconds_watched": 120, "video_seconds": 200},
        },
        "plan_changed": {"verb": "changed_plan", "object": {"plan": "free"}, "result": {}},
    }[kind]
    context: dict[str, Any] = {"platform": "ios"}
    if kind != "plan_changed":
        context["registration"] = attempt
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": kind,
        "schema_version": 1,
        "timestamp": ts,
        "actor": {"learner_id": learner, "class_id": class_id, "email": f"{learner}@school.fi"},
        "context": context,
        **body,
    }
    event.update(overrides)
    return event
