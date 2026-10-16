from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from conftest import make_event

from learning_events.cli import main
from learning_events.contracts import ContractRegistry, breaking_changes, check_registry

EVENT_TYPES = ["lesson_started", "lesson_completed", "quiz_answered", "video_watched"]


@pytest.mark.parametrize("event_type", [*EVENT_TYPES, "plan_changed"])
def test_valid_events_of_every_type_pass(registry: ContractRegistry, event_type: str) -> None:
    assert registry.validate(make_event(event_type)).ok


def test_lesson_completed_v2_accepts_new_optional_fields_and_v1_still_works(
    registry: ContractRegistry,
) -> None:
    v1 = make_event("lesson_completed")
    v2 = copy.deepcopy(v1) | {"schema_version": 2}
    v2["result"]["score_scaled"] = 0.8
    v2["context"]["app_version"] = "2.0.0"
    assert registry.validate(v1).ok and registry.validate(v2).ok
    v2["result"]["score_scaled"] = 1.5
    verdict = registry.validate(v2)
    assert verdict.reason == "schema_violation"
    assert any(e.startswith("result.score_scaled") for e in verdict.errors)


@pytest.mark.parametrize(
    ("event", "reason"),
    [
        ("not an object", "malformed_envelope"),
        ({"event_type": "lesson_started", "schema_version": "1"}, "malformed_envelope"),
        (make_event("lesson_started", event_type="lesson_paused"), "unknown_event_type"),
        (make_event("lesson_started", schema_version=3), "unsupported_schema_version"),
        (make_event("quiz_answered", verb="started"), "schema_violation"),
        (make_event("lesson_started", timestamp="02/03/2026 10:00"), "schema_violation"),
    ],
)
def test_rejections_have_a_stable_reason(registry: ContractRegistry, event, reason) -> None:
    verdict = registry.validate(event)
    assert not verdict.ok and verdict.reason == reason and verdict.errors


def test_error_messages_name_the_field(registry: ContractRegistry) -> None:
    event = make_event("lesson_completed")
    del event["actor"]["class_id"]
    event["result"]["duration_seconds"] = "600"
    errors = registry.validate(event).errors
    assert any(e.startswith("actor:") and "class_id" in e for e in errors)
    assert any(e.startswith("result.duration_seconds:") for e in errors)


def test_repository_contracts_are_backward_compatible(registry: ContractRegistry) -> None:
    """The CI gate: any breaking change to a committed contract fails this test."""
    report = check_registry(registry)
    assert report == {"lesson_completed v1->v2": []}
    assert main(["check-contracts"]) == 0


@pytest.fixture
def v1(registry: ContractRegistry) -> dict:
    return copy.deepcopy(registry.schemas[("lesson_completed", 1)])


def test_adding_an_optional_field_is_not_breaking(v1: dict) -> None:
    new = copy.deepcopy(v1)
    new["properties"]["result"]["properties"]["hints_used"] = {"type": "integer"}
    assert breaking_changes(v1, new) == []


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        (lambda s: s["properties"]["result"]["properties"].pop("success"), "removed or renamed"),
        (
            lambda s: s["properties"]["result"]["properties"].update(
                {"succeeded": s["properties"]["result"]["properties"].pop("success")}
            ),
            "removed or renamed: result.success",
        ),
        (
            lambda s: s["properties"]["result"]["properties"]["duration_seconds"].update(
                {"type": "string"}
            ),
            "type changed: result.duration_seconds",
        ),
        (
            lambda s: (
                s["properties"]["result"]["required"].append("hints_used")
                or s["properties"]["result"]["properties"].update(
                    {"hints_used": {"type": "integer"}}
                )
            ),
            "new required field: result.hints_used",
        ),
        (
            lambda s: s["properties"]["result"]["required"].remove("success"),
            "became optional: result.success",
        ),
        (
            lambda s: s["properties"]["context"]["properties"]["platform"].update(
                {"enum": ["web", "ios"]}
            ),
            "enum values removed: context.platform ['android']",
        ),
    ],
)
def test_breaking_changes_are_detected(v1: dict, change, expected: str) -> None:
    new = copy.deepcopy(v1)
    change(new)
    problems = breaking_changes(v1, new)
    assert any(expected in p for p in problems), problems


def test_registry_rejects_a_file_whose_name_and_version_disagree(tmp_path: Path) -> None:
    schema = json.loads(Path("contracts/lesson_started.v1.json").read_text())
    (tmp_path / "lesson_started.v2.json").write_text(json.dumps(schema))
    with pytest.raises(ValueError, match="schema_version const"):
        ContractRegistry.load(tmp_path)
