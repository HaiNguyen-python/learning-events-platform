"""Raw landing tables in DuckDB (local, tests, CI) or Postgres (docker-compose).

Both engines run the same SQL. The only differences are the connection, the parameter
placeholder and the JSON column type, which is all this class hides. dbt reads these tables
as sources; nothing here knows about the models.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

WAREHOUSE_ENV = "LEVENTS_WAREHOUSE"
DEFAULT_DUCKDB = Path("data/warehouse.duckdb")


@dataclass(frozen=True)
class RawEvent:
    event_id: str
    event_type: str
    schema_version: int
    event_ts: datetime
    learner_id: str
    class_id: str
    email_hash: str | None
    payload: dict[str, Any]
    received_at: datetime


@dataclass(frozen=True)
class DeadLetter:
    dead_letter_id: str
    received_at: datetime
    reason: str
    errors: list[str]
    event_id: str | None
    learner_id: str | None
    payload: Any


@dataclass(frozen=True)
class BatchStats:
    batch_id: str
    received_at: datetime
    received: int
    accepted: int
    duplicates: int
    rejected: int


def _ddl(json_type: str) -> list[str]:
    return [
        "CREATE SCHEMA IF NOT EXISTS raw",
        f"""CREATE TABLE IF NOT EXISTS raw.events (
            event_id VARCHAR PRIMARY KEY,
            event_type VARCHAR NOT NULL,
            schema_version INTEGER NOT NULL,
            event_ts TIMESTAMP NOT NULL,
            learner_id VARCHAR NOT NULL,
            class_id VARCHAR NOT NULL,
            email_hash VARCHAR,
            payload {json_type} NOT NULL,
            received_at TIMESTAMP NOT NULL)""",
        f"""CREATE TABLE IF NOT EXISTS raw.dead_letter (
            dead_letter_id VARCHAR PRIMARY KEY,
            received_at TIMESTAMP NOT NULL,
            reason VARCHAR NOT NULL,
            errors {json_type} NOT NULL,
            event_id VARCHAR,
            learner_id VARCHAR,
            payload {json_type})""",
        """CREATE TABLE IF NOT EXISTS raw.ingest_batches (
            batch_id VARCHAR PRIMARY KEY,
            received_at TIMESTAMP NOT NULL,
            received INTEGER NOT NULL,
            accepted INTEGER NOT NULL,
            duplicates INTEGER NOT NULL,
            rejected INTEGER NOT NULL)""",
        """CREATE TABLE IF NOT EXISTS raw.erased_learners (
            learner_hash VARCHAR PRIMARY KEY,
            erased_at TIMESTAMP NOT NULL)""",
    ]


# Column types for bulk inserts; the order is the insert order.
_COLUMNS: dict[str, dict[str, str]] = {
    "raw.events": {
        "event_id": "VARCHAR",
        "event_type": "VARCHAR",
        "schema_version": "INTEGER",
        "event_ts": "TIMESTAMP",
        "learner_id": "VARCHAR",
        "class_id": "VARCHAR",
        "email_hash": "VARCHAR",
        "payload": "JSON",
        "received_at": "TIMESTAMP",
    },
    "raw.dead_letter": {
        "dead_letter_id": "VARCHAR",
        "received_at": "TIMESTAMP",
        "reason": "VARCHAR",
        "errors": "JSON",
        "event_id": "VARCHAR",
        "learner_id": "VARCHAR",
        "payload": "JSON",
    },
    "raw.ingest_batches": {
        "batch_id": "VARCHAR",
        "received_at": "TIMESTAMP",
        "received": "INTEGER",
        "accepted": "INTEGER",
        "duplicates": "INTEGER",
        "rejected": "INTEGER",
    },
    "raw.erased_learners": {"learner_hash": "VARCHAR", "erased_at": "TIMESTAMP"},
}


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"not JSON serialisable: {type(value).__name__}")


class Warehouse:
    """Thin wrapper over a DB-API connection. Use `connect()` to build one.

    Bulk writes send a whole batch as *one* JSON parameter and unpack it in SQL
    (`from_json` in DuckDB, `jsonb_to_recordset` in Postgres). Binding one parameter per
    value made DuckDB's Python binding the bottleneck of the whole ingest.
    """

    def __init__(self, conn: Any, engine: str) -> None:
        self.conn = conn
        self.engine = engine
        self._json = "JSONB" if engine == "postgres" else "JSON"

    def execute(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        # Both connections run in autocommit mode; DuckDB returns itself, psycopg a cursor.
        if self.engine == "postgres":
            sql = sql.replace("?", "%s")
        cur = self.conn.execute(sql, list(params))
        rows = cur.fetchall() if cur.description else []
        return [tuple(r) for r in rows]

    def init_schema(self) -> None:
        for statement in _ddl(self._json):
            self.execute(statement)

    def _rows_from_json(self, columns: dict[str, str]) -> str:
        """SQL that turns one JSON-array parameter into rows with the given columns."""
        names = ", ".join(columns)
        if self.engine == "postgres":
            typed = ", ".join(f"{c} {t.replace('JSON', 'JSONB')}" for c, t in columns.items())
            return f"SELECT {names} FROM jsonb_to_recordset(CAST(? AS JSONB)) AS x({typed})"
        structure = json.dumps([columns])
        return (
            f"SELECT {names} FROM (SELECT unnest(from_json(CAST(? AS JSON), '{structure}'), "
            "recursive := true))"
        )

    def _insert_many(self, table: str, rows: list[dict[str, Any]]) -> None:
        """INSERT ... ON CONFLICT DO NOTHING, so a retried batch never creates duplicates."""
        if not rows:
            return
        columns = _COLUMNS[table]
        sql = (
            f"INSERT INTO {table} ({', '.join(columns)}) "
            f"{self._rows_from_json(columns)} ON CONFLICT DO NOTHING"
        )
        self.execute(sql, [json.dumps(rows, default=_json_default)])

    def _matching(self, table: str, column: str, values: Iterable[str]) -> set[str]:
        wanted = list(dict.fromkeys(values))
        if not wanted:
            return set()
        listed = (
            "SELECT jsonb_array_elements_text(CAST(? AS JSONB))"
            if self.engine == "postgres"
            else "SELECT unnest(from_json(CAST(? AS JSON), '[\"VARCHAR\"]'))"
        )
        sql = f"SELECT {column} FROM {table} WHERE {column} IN ({listed})"
        return {r[0] for r in self.execute(sql, [json.dumps(wanted)])}

    def existing_event_ids(self, event_ids: Iterable[str]) -> set[str]:
        return self._matching("raw.events", "event_id", event_ids)

    def erased_hashes(self, hashes: Iterable[str]) -> set[str]:
        return self._matching("raw.erased_learners", "learner_hash", hashes)

    def insert_events(self, events: list[RawEvent]) -> None:
        self._insert_many("raw.events", [asdict(e) for e in events])

    def insert_dead_letters(self, letters: list[DeadLetter]) -> None:
        self._insert_many("raw.dead_letter", [asdict(d) for d in letters])

    def record_batch(self, stats: BatchStats) -> None:
        self._insert_many("raw.ingest_batches", [asdict(stats)])

    def erase_learner(self, learner_id: str, learner_hash: str, at: datetime) -> dict[str, int]:
        """GDPR erasure: keep a tombstone so late events are refused, then delete raw rows.

        The tombstone is written first: if the process dies between the steps, new events are
        already refused and repeating the (idempotent) request finishes the deletes.
        """
        self._insert_many("raw.erased_learners", [{"learner_hash": learner_hash, "erased_at": at}])
        counts: dict[str, int] = {}
        for table in ("raw.events", "raw.dead_letter"):
            n = self.execute(f"SELECT count(*) FROM {table} WHERE learner_id = ?", [learner_id])
            self.execute(f"DELETE FROM {table} WHERE learner_id = ?", [learner_id])
            counts[table.split(".")[1]] = int(n[0][0])
        return counts

    def purge_older_than(
        self, events_before: datetime, dead_letters_before: datetime
    ) -> dict[str, int]:
        """Retention: drop learning events and dead letters older than their window.

        `plan_changed` events are kept while the account exists: they are the history behind
        the SCD2 learner dimension, and billing records have their own legal retention.
        """
        counts: dict[str, int] = {}
        for table, condition, cutoff in (
            ("raw.events", "event_ts < ? AND event_type <> 'plan_changed'", events_before),
            ("raw.dead_letter", "received_at < ?", dead_letters_before),
        ):
            n = self.execute(f"SELECT count(*) FROM {table} WHERE {condition}", [cutoff])
            self.execute(f"DELETE FROM {table} WHERE {condition}", [cutoff])
            counts[table.split(".")[1]] = int(n[0][0])
        return counts

    def close(self) -> None:
        self.conn.close()


def warehouse_url() -> str:
    return os.environ.get(WAREHOUSE_ENV, str(DEFAULT_DUCKDB))


def connect(url: str | Path | None = None) -> Warehouse:
    """`postgresql://...` opens Postgres; anything else is a DuckDB file path."""
    target = str(url if url is not None else warehouse_url())
    if target.startswith(("postgresql://", "postgres://")):
        import psycopg

        return Warehouse(psycopg.connect(target, autocommit=True), "postgres")
    import duckdb

    if target != ":memory:":
        Path(target).parent.mkdir(parents=True, exist_ok=True)
    return Warehouse(duckdb.connect(target), "duckdb")
