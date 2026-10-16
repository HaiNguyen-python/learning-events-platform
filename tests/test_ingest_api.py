from __future__ import annotations

import os
from datetime import datetime

import pytest
from conftest import SALT, make_event

from learning_events.contracts import ContractRegistry
from learning_events.ingest import Ingestor
from learning_events.pii import MissingSaltError, keyed_hash, salt_from_env
from learning_events.warehouse import Warehouse, connect

T0 = datetime(2026, 3, 2, 12)


def _count(wh: Warehouse, table: str) -> int:
    return int(wh.execute(f"SELECT count(*) FROM {table}")[0][0])


def test_replaying_a_batch_is_idempotent(client, warehouse: Warehouse) -> None:
    batch = [make_event("lesson_started"), make_event("quiz_answered")]
    first = client.post("/events", json=batch).json()
    second = client.post("/events", json=batch).json()
    assert (first["accepted"], first["duplicates"]) == (2, 0)
    assert (second["accepted"], second["duplicates"]) == (0, 2)
    assert _count(warehouse, "raw.events") == 2


def test_duplicate_inside_one_batch_is_kept_once(client, warehouse: Warehouse) -> None:
    event = make_event("lesson_started")
    body = client.post("/events", json=[event, event]).json()
    assert [r["status"] for r in body["results"]] == ["accepted", "duplicate"]
    assert _count(warehouse, "raw.events") == 1


def test_bad_events_go_to_dead_letter_with_reason_and_good_ones_still_land(
    client, warehouse: Warehouse
) -> None:
    missing = make_event("lesson_started")
    del missing["actor"]["class_id"]
    batch = [
        make_event("lesson_started"),
        missing,
        make_event("quiz_answered", event_type="lesson_paused"),
        make_event("lesson_completed", schema_version=9),
        make_event("video_watched", timestamp="2026-13-02T10:00:00Z"),  # month 13
        42,
    ]
    body = client.post("/events", json=batch).json()
    assert body["accepted"] == 1 and body["rejected"] == 5
    reasons = [r["reason"] for r in body["results"]]
    assert reasons == [
        None,
        "schema_violation",
        "unknown_event_type",
        "unsupported_schema_version",
        "schema_violation",
        "malformed_envelope",
    ]
    assert "class_id" in body["results"][1]["errors"][0]
    stored = dict(warehouse.execute("SELECT reason, count(*) FROM raw.dead_letter GROUP BY 1"))
    assert stored == {
        "schema_violation": 2,
        "unknown_event_type": 1,
        "unsupported_schema_version": 1,
        "malformed_envelope": 1,
    }


def test_resending_a_broken_event_does_not_grow_the_dead_letter_table(
    client, warehouse: Warehouse
) -> None:
    bad = make_event("lesson_started", schema_version=3)
    client.post("/events", json=[bad])
    client.post("/events", json=[bad])
    assert _count(warehouse, "raw.dead_letter") == 1


def test_raw_email_is_never_stored_in_any_table(client, warehouse: Warehouse) -> None:
    """Scan every text/JSON column of every raw table for the plain address."""
    bad = make_event("quiz_answered", learner="L7")
    bad["result"]["note"] = "contact L7@school.fi"  # PII in an unexpected place
    bad["schema_version"] = 7  # and the event is dead-lettered
    client.post("/events", json=[make_event("lesson_started", learner="L7"), bad])
    columns = warehouse.execute(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND data_type IN ('VARCHAR', 'JSON')"
    )
    assert len(columns) >= 10
    for table, column in columns:
        hits = warehouse.execute(
            f"SELECT count(*) FROM raw.{table} WHERE CAST({column} AS VARCHAR) LIKE '%@%'"
        )
        assert hits[0][0] == 0, f"raw.{table}.{column} contains an email"
    email_hash = warehouse.execute("SELECT email_hash FROM raw.events")[0][0]
    assert email_hash == keyed_hash("L7@school.fi", SALT) != keyed_hash("L7@school.fi", "other")


def test_erasure_deletes_raw_data_and_refuses_late_events(client, warehouse: Warehouse) -> None:
    keep = make_event("lesson_started", learner="L2")
    broken = make_event("lesson_started", learner="L1", schema_version=5)
    client.post("/events", json=[make_event("lesson_started", learner="L1"), broken, keep])

    response = client.delete("/learners/L1")
    assert response.status_code == 200
    assert response.json()["events_deleted"] == 1
    assert response.json()["dead_letters_deleted"] == 1
    assert warehouse.execute("SELECT DISTINCT learner_id FROM raw.events") == [("L2",)]

    late = client.post("/events", json=[make_event("quiz_answered", learner="L1")]).json()
    assert late["results"][0]["reason"] == "learner_erased"
    leftovers = warehouse.execute("SELECT learner_id, payload FROM raw.dead_letter")
    assert leftovers == [(None, None)]  # the refusal is logged without the learner's data
    again = client.delete("/learners/L1").json()
    assert again["events_deleted"] == 0  # idempotent


def test_oversized_batch_is_refused(client) -> None:
    assert client.post("/events", json=[{}] * 1001).status_code == 413
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/contracts").json()["lesson_completed"] == [1, 2]


def test_retention_keeps_plan_history(ingestor: Ingestor, warehouse: Warehouse) -> None:
    old = [
        make_event("plan_changed", ts="2025-01-01T08:00:00Z"),
        make_event("lesson_started", ts="2025-01-02T08:00:00Z"),
        make_event("lesson_started", ts="2026-03-01T08:00:00Z", attempt="a2"),
    ]
    ingestor.ingest(old + [make_event("lesson_started", schema_version=4)], datetime(2025, 1, 3))
    counts = warehouse.purge_older_than(datetime(2025, 6, 1), datetime(2025, 6, 1))
    assert counts == {"events": 1, "dead_letter": 1}
    left = warehouse.execute("SELECT event_type FROM raw.events ORDER BY event_ts")
    assert left == [("plan_changed",), ("lesson_started",)]


def test_salt_must_be_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LEVENTS_PII_SALT", "short")
    with pytest.raises(MissingSaltError):
        salt_from_env()


@pytest.mark.skipif("POSTGRES_TEST_DSN" not in os.environ, reason="needs a Postgres server")
def test_postgres_backend_has_the_same_behaviour(registry: ContractRegistry) -> None:
    wh = connect(os.environ["POSTGRES_TEST_DSN"])
    wh.init_schema()
    for table in ("events", "dead_letter", "ingest_batches", "erased_learners"):
        wh.execute(f"DELETE FROM raw.{table}")
    ing = Ingestor(wh, registry, SALT)
    batch = [
        make_event("lesson_started", learner="P1"),
        make_event("lesson_started", event_type="nope"),
    ]
    first = ing.ingest(batch, T0)
    second = ing.ingest(batch, T0)
    assert (first.count("accepted"), first.count("rejected")) == (1, 1)
    assert (second.count("duplicate"), _count(wh, "raw.dead_letter")) == (1, 1)
    assert wh.erase_learner("P1", keyed_hash("P1", SALT), T0)["events"] == 1
    assert wh.erased_hashes([keyed_hash("P1", SALT)]) == {keyed_hash("P1", SALT)}
    wh.close()
