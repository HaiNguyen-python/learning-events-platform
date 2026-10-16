"""Seeded synthetic learning events that behave like a real classroom app.

Effects planted on purpose (so the models and the report have something real to find):
- school classes learn on weekdays (in class 9-14 and homework 17-21), the adult evening class
  learns after 18:00 and at weekends (Europe/Helsinki time);
- every learner can drop out; the adult class `AD1` has a much higher weekly drop-out hazard;
- 2% of events are re-sent with the same event_id (client retry after a timeout);
- 1% of app events are malformed in one of five ways;
- 4% of mobile events arrive 6-72 hours late (offline sync);
- app release 2.0 rolls out from week 5 and switches `lesson_completed` to contract v2.

Plan changes (signup, upgrade, downgrade) come from the billing backend, so they are never
malformed or late. That is also true in most real products.
"""

from __future__ import annotations

import copy
import math
import random
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("Europe/Helsinki")
MALFORMATIONS = (
    "missing_class_id",
    "bad_timestamp",
    "unknown_event_type",
    "unsupported_version",
    "wrong_type",
)


@dataclass(frozen=True)
class ClassProfile:
    class_id: str
    kind: str  # "school" or "adult"
    weekly_dropout: float
    skill: float  # probability of a correct quiz answer for an average learner
    completion: float  # probability of finishing a started lesson


DEFAULT_CLASSES = (
    ClassProfile("7A", "school", 0.03, 0.72, 0.88),
    ClassProfile("7B", "school", 0.04, 0.68, 0.86),
    ClassProfile("8A", "school", 0.04, 0.75, 0.87),
    ClassProfile("8B", "school", 0.05, 0.66, 0.84),
    ClassProfile("9A", "school", 0.05, 0.78, 0.89),
    ClassProfile("AD1", "adult", 0.16, 0.70, 0.72),
)


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 7
    start: date = date(2026, 3, 2)  # a Monday
    weeks: int = 8
    learners_per_class: int = 24
    classes: tuple[ClassProfile, ...] = DEFAULT_CLASSES
    duplicate_rate: float = 0.02
    malformed_rate: float = 0.01
    late_rate: float = 0.04
    v2_rollout_week: int = 4  # 0-based: the release starts on the first day of week 5
    upgrade_share: float = 0.25
    downgrade_share: float = 0.3  # of upgraders


@dataclass(frozen=True)
class Delivery:
    """One HTTP delivery: when the server received it and the JSON the client sent."""

    delivered_at: datetime  # naive UTC
    event: dict[str, Any]


@dataclass
class GeneratedData:
    deliveries: list[Delivery]
    planted: Counter[str] = field(default_factory=Counter)


@dataclass
class _Learner:
    learner_id: str
    email: str
    profile: ClassProfile
    platform: str
    motivation: float
    dropout_week: int
    upgrade_at: datetime  # when this learner's app updates to 2.0 (naive UTC)
    lesson_counter: int = 0


def _geometric(rng: random.Random, p: float) -> int:
    """Number of weekly trials until the first drop-out (1, 2, ...), like numpy's geometric."""
    return max(1, math.ceil(math.log(1.0 - rng.random()) / math.log(1.0 - p)))


def _clip(value: float, low: float, high: float) -> float:
    return min(high, max(low, value))


def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_utc(day: date, hour: int, minute: int) -> datetime:
    local = datetime.combine(day, time(hour, minute), tzinfo=LOCAL_TZ)
    return local.astimezone(UTC).replace(tzinfo=None)


class _Builder:
    """Holds the RNG and turns learner behaviour into xAPI-style events."""

    def __init__(self, cfg: GeneratorConfig) -> None:
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)

    def uid(self) -> str:
        return str(uuid.UUID(bytes=self.rng.randbytes(16), version=4))

    def learners(self) -> list[_Learner]:
        out = []
        rollout_start = datetime.combine(self.cfg.start, time()) + timedelta(
            weeks=self.cfg.v2_rollout_week
        )
        n = 0
        for profile in self.cfg.classes:
            for _ in range(self.cfg.learners_per_class):
                n += 1
                platform = self.rng.choices(["web", "ios", "android"], weights=[0.4, 0.3, 0.3])[0]
                # Geometric drop-out: week index in which the learner stops for good.
                dropout = _geometric(self.rng, profile.weekly_dropout)
                out.append(
                    _Learner(
                        learner_id=f"L{n:04d}",
                        email=f"learner{n:04d}@example.com",
                        profile=profile,
                        platform=platform,
                        motivation=_clip(self.rng.lognormvariate(0, 0.35), 0.4, 1.8),
                        dropout_week=dropout,
                        upgrade_at=rollout_start + timedelta(hours=self.rng.uniform(0, 14 * 24)),
                    )
                )
        return out

    def envelope(
        self, learner: _Learner, event_type: str, verb: str, ts: datetime, version: int = 1
    ) -> dict[str, Any]:
        return {
            "event_id": self.uid(),
            "event_type": event_type,
            "schema_version": version,
            "timestamp": _iso(ts),
            "actor": {
                "learner_id": learner.learner_id,
                "class_id": learner.profile.class_id,
                "email": learner.email,
            },
            "verb": verb,
            "object": {},
            "context": {"platform": learner.platform},
        }

    def plan_events(self, learner: _Learner) -> list[dict[str, Any]]:
        term_start = datetime.combine(self.cfg.start, time(8))
        signup = term_start - timedelta(hours=self.rng.uniform(24, 14 * 24))
        changes: list[tuple[datetime, str, str | None]] = [(signup, "free", None)]
        if self.rng.random() < self.cfg.upgrade_share:
            up = term_start + timedelta(days=self.rng.uniform(3, max(4, 7 * self.cfg.weeks - 14)))
            changes.append((up, "premium", "free"))
            if self.rng.random() < self.cfg.downgrade_share:
                down = up + timedelta(days=self.rng.uniform(7, 14))
                changes.append((down, "free", "premium"))
        events = []
        for ts, plan, previous in changes:
            e = self.envelope(learner, "plan_changed", "changed_plan", ts)
            e["object"] = {"plan": plan}
            e["result"] = {"previous_plan": previous} if previous else {}
            events.append(e)
        return events

    def session_slots(self, learner: _Learner, day: date) -> list[tuple[int, int]]:
        """Start times (local hour, minute) of the sessions on one day, possibly none."""
        weekend = day.weekday() >= 5
        if learner.profile.kind == "school":
            p_active = 0.15 if weekend else 0.55
            slots = [(10, 19)] if weekend else [(9, 14), (17, 21)]
        else:
            p_active = 0.40 if weekend else 0.32
            slots = [(9, 19)] if weekend else [(18, 22)]
        if self.rng.random() > min(0.95, p_active * learner.motivation):
            return []
        n_sessions = 2 if (len(slots) == 2 and self.rng.random() < 0.3) else 1
        chosen = slots if n_sessions == 2 else [slots[self.rng.randrange(len(slots))]]
        return [(self.rng.randrange(lo, hi), self.rng.randrange(0, 60)) for lo, hi in chosen]

    def attempt_event(
        self,
        learner: _Learner,
        attempt: str,
        event_type: str,
        verb: str,
        ts: datetime,
        version: int = 1,
    ) -> dict[str, Any]:
        """An event that belongs to one lesson attempt (xAPI context.registration)."""
        event = self.envelope(learner, event_type, verb, ts, version)
        event["context"]["registration"] = attempt
        return event

    def session(self, learner: _Learner, start: datetime) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        ts = start
        course = f"ENG-{learner.profile.class_id}"
        for _ in range(self.rng.randrange(1, 3)):
            learner.lesson_counter += 1
            lesson = f"{course}-L{learner.lesson_counter:02d}"
            attempt = self.uid()

            start_event = self.attempt_event(learner, attempt, "lesson_started", "started", ts)
            start_event["object"] = {"lesson_id": lesson, "course_id": course}
            events.append(start_event)
            started = ts
            ts += timedelta(seconds=self.rng.randrange(30, 150))
            if self.rng.random() < 0.6:
                length = self.rng.randrange(180, 420)
                watched = (
                    length if self.rng.random() < 0.7 else int(length * self.rng.uniform(0.2, 0.9))
                )
                ts += timedelta(seconds=watched)
                v = self.attempt_event(learner, attempt, "video_watched", "watched", ts)
                events.append(v)
                v["object"] = {"video_id": f"{lesson}-V1", "lesson_id": lesson}
                v["result"] = {"seconds_watched": watched, "video_seconds": length}
            p_correct = _clip(learner.profile.skill * learner.motivation**0.3, 0.05, 0.97)
            scores = []
            for q in range(self.rng.randrange(4, 7)):
                ts += timedelta(seconds=self.rng.randrange(20, 90))
                correct = bool(self.rng.random() < p_correct)
                score = 1.0 if correct else self.rng.choice([0.0, 0.5])
                scores.append(score)
                quiz = self.attempt_event(learner, attempt, "quiz_answered", "answered", ts)
                events.append(quiz)
                quiz["object"] = {"lesson_id": lesson, "question_id": f"{lesson}-Q{q + 1}"}
                quiz["result"] = {
                    "success": correct,
                    "score_scaled": score,
                    "response_ms": self.rng.randrange(2000, 40000),
                }
            if self.rng.random() > learner.profile.completion * min(1.0, learner.motivation):
                break  # abandoned: the learner closes the app mid-lesson
            ts += timedelta(seconds=self.rng.randrange(10, 60))
            v2 = ts >= learner.upgrade_at
            done = self.attempt_event(
                learner, attempt, "lesson_completed", "completed", ts, version=2 if v2 else 1
            )
            events.append(done)
            done["object"] = {"lesson_id": lesson, "course_id": course}
            done["result"] = {
                "duration_seconds": int((ts - started).total_seconds()),
                "success": sum(scores) / len(scores) >= 0.6,
            }
            if v2:
                done["result"]["score_scaled"] = round(sum(scores) / len(scores), 3)
                done["context"]["app_version"] = "2.0.0"
            ts += timedelta(seconds=self.rng.randrange(30, 300))
        return events

    def learner_events(self, learner: _Learner) -> list[dict[str, Any]]:
        events = []
        for day_offset in range(7 * self.cfg.weeks):
            if day_offset // 7 >= learner.dropout_week:
                break
            day = self.cfg.start + timedelta(days=day_offset)
            for hour, minute in self.session_slots(learner, day):
                events.extend(self.session(learner, _to_utc(day, hour, minute)))
        return events

    def malform(self, event: dict[str, Any], kind: str) -> dict[str, Any]:
        bad = copy.deepcopy(event)
        if kind == "missing_class_id":
            del bad["actor"]["class_id"]
        elif kind == "bad_timestamp":
            bad["timestamp"] = datetime.fromisoformat(event["timestamp"][:-1]).strftime(
                "%d/%m/%Y %H:%M"
            )
        elif kind == "unknown_event_type":
            bad["event_type"] = "lesson_paused"
        elif kind == "unsupported_version":
            bad["schema_version"] = 3
        else:  # wrong_type: an old Android build sent numbers as strings
            bad["actor"]["learner_id"] = int(bad["actor"]["learner_id"][1:])
        return bad


def generate(cfg: GeneratorConfig | None = None) -> GeneratedData:
    """Build every delivery the API will receive, sorted by arrival time."""
    cfg = cfg or GeneratorConfig()
    b = _Builder(cfg)
    planted: Counter[str] = Counter()
    deliveries: list[Delivery] = []
    for learner in b.learners():
        for e in b.plan_events(learner):
            ts = datetime.fromisoformat(e["timestamp"][:-1])
            deliveries.append(Delivery(ts + timedelta(seconds=1), e))
        for e in b.learner_events(learner):
            ts = datetime.fromisoformat(e["timestamp"][:-1])
            delay = timedelta(seconds=b.rng.uniform(0.2, 5))
            if learner.platform != "web" and b.rng.random() < cfg.late_rate:
                delay = timedelta(hours=b.rng.uniform(6, 72))
                planted["late"] += 1
            sent = e
            if b.rng.random() < cfg.malformed_rate:
                kind = MALFORMATIONS[b.rng.randrange(len(MALFORMATIONS))]
                sent = b.malform(e, kind)
                planted[f"malformed:{kind}"] += 1
                if delay > timedelta(hours=1):
                    planted["late_and_malformed"] += 1
            deliveries.append(Delivery(ts + delay, sent))
            if sent is e and b.rng.random() < cfg.duplicate_rate:
                retry = timedelta(seconds=b.rng.uniform(5, 60))
                deliveries.append(Delivery(ts + delay + retry, copy.deepcopy(e)))
                planted["duplicate"] += 1
    deliveries.sort(key=lambda d: d.delivered_at)
    planted["events_sent"] = len(deliveries)
    return GeneratedData(deliveries, planted)


def batches(
    deliveries: list[Delivery], size: int = 500, window: timedelta = timedelta(minutes=30)
) -> list[list[Delivery]]:
    """Group deliveries into API calls: what arrived in the same 30-minute window, max `size`.

    Windowing keeps each batch's simulated arrival time close to its events' delivery time,
    so "late" in the warehouse means late on the device, not late because of batching.
    """
    out: list[list[Delivery]] = []
    current: list[Delivery] = []
    window_end: datetime | None = None
    for d in deliveries:
        if current and (len(current) >= size or (window_end and d.delivered_at >= window_end)):
            out.append(current)
            current = []
        if not current:
            window_end = d.delivered_at + window
        current.append(d)
    if current:
        out.append(current)
    return out
