"""Ingestion core: validate a batch, drop duplicates, pseudonymise, write raw or dead-letter.

Kept separate from the FastAPI layer so the same rules run in the API, the CLI and tests.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

from .contracts import ContractRegistry, Verdict
from .pii import keyed_hash, pseudonymise_event, scrub
from .warehouse import BatchStats, DeadLetter, RawEvent, Warehouse

Status = Literal["accepted", "duplicate", "rejected"]


@dataclass(frozen=True)
class EventResult:
    index: int
    event_id: str | None
    status: Status
    reason: str | None = None
    errors: tuple[str, ...] = ()


@dataclass
class BatchResult:
    batch_id: str
    results: list[EventResult] = field(default_factory=list)

    def count(self, status: Status) -> int:
        return sum(r.status == status for r in self.results)


def parse_timestamp(value: str) -> datetime:
    """RFC 3339 string -> naive UTC datetime (the warehouse stores UTC without zone)."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp needs a UTC offset")
    return parsed.astimezone(UTC).replace(tzinfo=None)


def _event_id(event: Any) -> str | None:
    value = event.get("event_id") if isinstance(event, dict) else None
    return value if isinstance(value, str) else None


def _learner_id(event: Any) -> str | None:
    actor = event.get("actor") if isinstance(event, dict) else None
    value = actor.get("learner_id") if isinstance(actor, dict) else None
    return value if isinstance(value, str) else None


class Ingestor:
    """Applies the contract and privacy rules to one batch at a time."""

    def __init__(self, warehouse: Warehouse, registry: ContractRegistry, salt: str) -> None:
        self.warehouse = warehouse
        self.registry = registry
        self.salt = salt

    def ingest(self, batch: list[Any], received_at: datetime) -> BatchResult:
        result = BatchResult(batch_id=str(uuid.uuid4()))
        verdicts = [self._check(event) for event in batch]
        valid_ids = [_event_id(e) for e, v in zip(batch, verdicts, strict=True) if v.ok]
        already_stored = self.warehouse.existing_event_ids(i for i in valid_ids if i)
        erased = self.warehouse.erased_hashes(
            keyed_hash(lid, self.salt)
            for e, v in zip(batch, verdicts, strict=True)
            if v.ok and (lid := _learner_id(e))
        )
        seen_in_batch: set[str] = set()
        accepted: list[RawEvent] = []
        dead: list[DeadLetter] = []
        for index, (event, verdict) in enumerate(zip(batch, verdicts, strict=True)):
            event_id = _event_id(event)
            if verdict.ok:
                assert event_id is not None
                learner_id = _learner_id(event)
                if event_id in already_stored or event_id in seen_in_batch:
                    result.results.append(EventResult(index, event_id, "duplicate"))
                    continue
                if learner_id is not None and keyed_hash(learner_id, self.salt) in erased:
                    verdict = Verdict(False, "learner_erased", ("learner asked to be erased",))
                else:
                    seen_in_batch.add(event_id)
                    accepted.append(self._to_raw(event, received_at))
                    result.results.append(EventResult(index, event_id, "accepted"))
                    continue
            dead.append(self._to_dead_letter(event, verdict, received_at))
            result.results.append(
                EventResult(index, event_id, "rejected", verdict.reason, verdict.errors)
            )
        self.warehouse.insert_events(accepted)
        self.warehouse.insert_dead_letters(dead)
        self.warehouse.record_batch(
            BatchStats(
                result.batch_id,
                received_at,
                len(batch),
                result.count("accepted"),
                result.count("duplicate"),
                result.count("rejected"),
            )
        )
        return result

    def _check(self, event: Any) -> Verdict:
        verdict = self.registry.validate(event)
        if not verdict.ok:
            return verdict
        try:
            parse_timestamp(event["timestamp"])
        except ValueError as exc:  # the pattern allows e.g. month 13; the parser does not
            return Verdict(False, "schema_violation", (f"timestamp: {exc}",))
        return verdict

    def _to_raw(self, event: dict[str, Any], received_at: datetime) -> RawEvent:
        payload, email_hash = pseudonymise_event(event, self.salt)
        return RawEvent(
            event_id=event["event_id"],
            event_type=event["event_type"],
            schema_version=event["schema_version"],
            event_ts=parse_timestamp(event["timestamp"]),
            learner_id=event["actor"]["learner_id"],
            class_id=event["actor"]["class_id"],
            email_hash=email_hash,
            payload=payload,
            received_at=received_at,
        )

    def _to_dead_letter(self, event: Any, verdict: Verdict, received_at: datetime) -> DeadLetter:
        erased = verdict.reason == "learner_erased"
        # Content-derived id: re-sending the same broken event does not add a second row.
        content = json.dumps([verdict.reason, event], sort_keys=True, default=str)
        return DeadLetter(
            dead_letter_id=keyed_hash(content, self.salt)[:32],
            received_at=received_at,
            reason=verdict.reason or "unknown",
            errors=[scrub(e, self.salt) for e in verdict.errors],
            event_id=_event_id(event),
            # An erased learner's data must not come back in through the dead-letter table.
            learner_id=None if erased else _learner_id(event),
            payload=None if erased else scrub(event, self.salt),
        )
