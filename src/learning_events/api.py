"""FastAPI service: `POST /events` (batch ingest) and `DELETE /learners/{id}` (GDPR erasure).

The ingestor and the clock are passed in, so tests and the in-process pipeline can swap the
warehouse or replay events with a simulated arrival time.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from .ingest import Ingestor
from .pii import keyed_hash

MAX_BATCH = 1000


class EventOutcome(BaseModel):
    index: int
    event_id: str | None
    status: str
    reason: str | None = None
    errors: list[str] = []


class IngestResponse(BaseModel):
    batch_id: str
    received: int
    accepted: int
    duplicates: int
    rejected: int
    results: list[EventOutcome]


class ErasureResponse(BaseModel):
    learner_id: str
    events_deleted: int
    dead_letters_deleted: int
    note: str


def utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def create_app(ingestor: Ingestor, clock: Callable[[], datetime] = utc_now) -> FastAPI:
    app = FastAPI(title="Learning events ingestion API", version="0.1.0")

    def get_ingestor() -> Ingestor:
        return ingestor

    def get_clock() -> Callable[[], datetime]:
        return clock

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/contracts")
    def contracts(ing: Ingestor = Depends(get_ingestor)) -> dict[str, list[int]]:
        registry = ing.registry
        return {t: registry.versions(t) for t in registry.event_types}

    @app.post("/events", response_model=IngestResponse)
    def post_events(
        batch: list[Any],
        ing: Ingestor = Depends(get_ingestor),
        now: Callable[[], datetime] = Depends(get_clock),
    ) -> IngestResponse:
        """Accepts a JSON array. Always 200 with a result per event, so one bad event in a
        batch of 500 does not make the client resend the 499 good ones."""
        if len(batch) > MAX_BATCH:
            raise HTTPException(413, f"batch too large: {len(batch)} > {MAX_BATCH} events")
        result = ing.ingest(batch, received_at=now())
        return IngestResponse(
            batch_id=result.batch_id,
            received=len(batch),
            accepted=result.count("accepted"),
            duplicates=result.count("duplicate"),
            rejected=result.count("rejected"),
            results=[
                EventOutcome(
                    index=r.index,
                    event_id=r.event_id,
                    status=r.status,
                    reason=r.reason,
                    errors=list(r.errors),
                )
                for r in result.results
            ],
        )

    @app.delete("/learners/{learner_id}", response_model=ErasureResponse)
    def erase_learner(
        learner_id: str,
        ing: Ingestor = Depends(get_ingestor),
        now: Callable[[], datetime] = Depends(get_clock),
    ) -> ErasureResponse:
        """Idempotent: a second call deletes nothing and still returns 200."""
        counts = ing.warehouse.erase_learner(learner_id, keyed_hash(learner_id, ing.salt), now())
        return ErasureResponse(
            learner_id=learner_id,
            events_deleted=counts["events"],
            dead_letters_deleted=counts["dead_letter"],
            note="Raw data deleted. Derived dbt models drop this learner on the next build.",
        )

    return app
