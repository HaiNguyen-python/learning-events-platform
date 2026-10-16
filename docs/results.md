# Results

Data: **Synthetic data (seed 7, 6 classes x 24 learners, 8 weeks)**.

Produced by `levents report` from the warehouse and the last `dbt build`;
rerun `levents run-all` to regenerate everything.

## Ingestion

| Step | Events |
| --- | --- |
| Received by `POST /events` (1493 batches) | 40,048 |
| Accepted into `raw.events` | 38,895 |
| Duplicates dropped (same `event_id`) | 763 |
| Rejected to `raw.dead_letter` | 390 |
| Accepted events that arrived more than 2 hours late | 871 |

| Dead-letter reason | Events |
| --- | --- |
| `schema_violation` | 233 |
| `unknown_event_type` | 81 |
| `unsupported_schema_version` | 76 |

Planted by the generator vs. found by the pipeline:

| Planted problem | Planted | Found |
| --- | --- | --- |
| Re-sent duplicates | 763 | 763 dropped |
| Malformed events | 390 | 390 rejected |
| Late mobile events (6-72 h) | 877 (6 of them also malformed) | 871 late in `raw.events` |

## Contract versions in raw.events

| Event type | Version | Events |
| --- | --- | --- |
| lesson_completed | v1 | 2,791 |
| lesson_completed | v2 | 1,413 |
| lesson_started | v1 | 5,232 |
| plan_changed | v1 | 204 |
| quiz_answered | v1 | 26,077 |
| video_watched | v1 | 3,178 |

## dbt models (rows)

| Model | Rows |
| --- | --- |
| `stg_lesson_started` | 5,232 |
| `stg_lesson_completed` | 4,204 |
| `stg_quiz_answered` | 26,077 |
| `stg_video_watched` | 3,178 |
| `stg_plan_changed` | 204 |
| `int_sessions` | 3,749 |
| `dim_learner` | 204 |
| `fct_lesson_attempts` | 5,232 |
| `fct_daily_learner_activity` | 3,006 |
| `mart_teacher_weekly` | 48 |

## dbt build

| Node type | Pass / OK | Warn | Fail / Error |
| --- | --- | --- | --- |
| Models | 10 | - | 0 |
| Data tests | 38 | 1 | 0 |

Warning: `relationships_stg_lesson_completed_attempt_id__attempt_id__ref_stg_lesson_started_` found 35 rows (severity warn). These are completions whose `lesson_started` event was malformed and dead-lettered, so the attempt has no start. `fct_lesson_attempts` is driven by starts and skips them.

## Teacher view (mart_teacher_weekly)

**Class AD1 lost 70% of its weekly active learners between the first and last week; the other classes lost 27% on average.**

| Class | Active learners week 1 -> week 8 | Change | Lesson completion | Avg quiz score | At risk in last week |
| --- | --- | --- | --- | --- | --- |
| 7A | 23 -> 17 | -26% | 81.8% | 80.8% | 7 |
| 7B | 21 -> 15 | -29% | 82.3% | 79.5% | 9 |
| 8A | 24 -> 17 | -29% | 82.3% | 85.7% | 7 |
| 8B | 23 -> 17 | -26% | 76.8% | 75.6% | 7 |
| 9A | 22 -> 17 | -23% | 79.9% | 84.0% | 7 |
| AD1 | 23 -> 7 | -70% | 66.8% | 81.0% | 17 |
