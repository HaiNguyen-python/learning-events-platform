# How it works

This page follows one learning event from the app to the teacher's weekly report and explains
the important code on the way. Python files are in `src/learning_events/`, SQL in `dbt/`.

## 1. The event format (xAPI style)

Every event has the same envelope. A completed lesson looks like this:

```json
{
  "event_id": "5b0d...e1",
  "event_type": "lesson_completed",
  "schema_version": 2,
  "timestamp": "2026-03-30T15:04:11Z",
  "actor":   {"learner_id": "L0042", "class_id": "8B", "email": "learner0042@example.com"},
  "verb":    "completed",
  "object":  {"lesson_id": "ENG-8B-L07", "course_id": "ENG-8B"},
  "result":  {"duration_seconds": 512, "success": true, "score_scaled": 0.8},
  "context": {"registration": "9c1f...", "platform": "android", "app_version": "2.0.0"}
}
```

`context.registration` is the xAPI name for an attempt id. Every event of one lesson attempt
(start, video, quiz answers, completion) carries it, which makes the attempt easy to rebuild in
SQL later. `event_id` is generated on the device and is the idempotency key.

## 2. Contracts: `contracts/*.json` and `contracts.py`

There is one JSON Schema per event type and version: `lesson_started.v1.json`,
`lesson_completed.v1.json`, `lesson_completed.v2.json`, `quiz_answered.v1.json`,
`video_watched.v1.json`, `plan_changed.v1.json`.

`ContractRegistry.load()` reads them, checks that each is a valid Draft 2020-12 schema and that
the file name agrees with the `event_type` and `schema_version` constants inside it.

`ContractRegistry.validate(event)` returns a `Verdict` with a short **reason code** and readable
errors. The order of checks matters, because the reason is what operations staff see first:

| Check | Reason code |
| --- | --- |
| Not a JSON object, or `event_type` / `schema_version` missing or wrong type | `malformed_envelope` |
| No contract for this event type | `unknown_event_type` |
| Event type known, version not registered | `unsupported_schema_version` |
| JSON Schema errors (missing field, wrong type, out of range, bad timestamp) | `schema_violation` |

Errors name the field path, for example `result.duration_seconds: '600' is not of type
'integer'`, so the client team can fix their code from the API response alone.

### Compatibility checker

`flatten(schema)` turns a schema into `{field path: FieldSpec(types, required, enum)}`. A nested
field counts as required only if its parents are required too. `breaking_changes(old, new)`
compares two flattened schemas:

| Change | Verdict |
| --- | --- |
| New optional field | fine |
| Optional field removed | fine (consumers already handle it missing) |
| Required field removed or renamed | breaking |
| Required field became optional | breaking |
| New required field | breaking (old producers do not send it) |
| Type changed | breaking |
| Enum value removed | breaking |

`check_registry` compares every version with the one before it. `levents check-contracts` exits
with code 1 on any breaking change, and `tests/test_contracts.py ->
test_repository_contracts_are_backward_compatible` makes the same check part of pytest, so a
pull request that breaks a contract fails CI.

**How this mirrors a schema registry.** In Confluent Schema Registry (or Apicurio, AWS Glue) a
*subject* has numbered, immutable *versions*, producers tag each message with the schema id, and
the registry refuses a new version that fails the subject's compatibility mode. Here the subject
is the event type, the version is in the file name and in `schema_version`, and the CI test plays
the registry's role. The rule is close to Confluent's FULL mode for required fields, because both
old app builds (producers) and the dbt models (consumers) must keep working.

The v2 change in this repo adds `result.score_scaled` and `context.app_version`, both optional.
`stg_lesson_completed` reads both versions in one query; v1 rows get nulls. No model changed.

## 3. Ingestion: `ingest.py` and `api.py`

`POST /events` takes a JSON array (at most 1000 events, otherwise 413) and always answers 200
with one result per event:

```json
{"batch_id": "...", "received": 500, "accepted": 491, "duplicates": 6, "rejected": 3,
 "results": [{"index": 17, "event_id": "...", "status": "rejected",
              "reason": "schema_violation", "errors": ["actor: 'class_id' is a required property"]}]}
```

`Ingestor.ingest(batch, received_at)` does the work:

1. **Validate** every event with the registry, then parse the timestamp (the schema pattern
   allows month 13; `datetime.fromisoformat` does not).
2. **Look up** which valid `event_id`s are already stored (`existing_event_ids`) and which
   learners were erased (`erased_hashes`), each in one query.
3. **Decide per event**: duplicate (already stored or seen earlier in this batch), refused
   (`learner_erased`), accepted, or rejected.
4. **Pseudonymise** accepted events (`pii.pseudonymise_event`): the email moves out of the
   payload into an `email_hash` column, and every other string that looks like an email is
   replaced by its hash too.
5. **Write** accepted events to `raw.events`, rejected ones to `raw.dead_letter` (scrubbed the
   same way), and one row of counts to `raw.ingest_batches`.

The API layer is thin: `create_app(ingestor, clock)` wires routes to an `Ingestor` and a clock.
Passing the clock in lets the pipeline replay two months of synthetic traffic in two minutes
with realistic arrival times.

### Idempotency

Two layers. The pre-check in step 2 gives honest numbers in the response. The insert itself is
`INSERT ... ON CONFLICT DO NOTHING` on the `event_id` primary key, so two API workers handling
the same retry at the same moment still store one row. Dead letters get a content-derived id
(keyed hash of reason + payload), so a client that keeps resending the same broken event does not
fill the table. Tests: `test_replaying_a_batch_is_idempotent`,
`test_duplicate_inside_one_batch_is_kept_once`,
`test_resending_a_broken_event_does_not_grow_the_dead_letter_table`.

### Fast bulk writes

`Warehouse._insert_many` sends a whole batch as **one JSON parameter** and unpacks it in SQL:
`unnest(from_json(?, '<structure>'))` in DuckDB, `jsonb_to_recordset(?)` in Postgres. In a profiling run, binding one parameter per value made
the whole DuckDB ingest of about 37,000 events take 93 s; one JSON parameter per batch took 38 s.

## 4. Privacy: `pii.py`, erasure and retention

- **Pseudonymisation.** `keyed_hash` is HMAC-SHA256 over the lower-cased email with a secret
  salt from `LEVENTS_PII_SALT`. The same email always gives the same key (needed to join a
  learner's events), but without the salt nobody can hash a list of known emails to find a
  match. There is no default salt: `salt_from_env` raises if it is missing or short.
- **Why this matters under GDPR.** Data minimisation (Art. 5(1)(c)) says to store only what the
  purpose needs; analytics needs a stable key, not the address. Pseudonymisation (Art. 4(5),
  Art. 32) lowers the risk if the warehouse leaks. Pseudonymised data is still personal data, so
  the rights below still apply.
- **Right to erasure (Art. 17).** `DELETE /learners/{id}` calls `Warehouse.erase_learner`. It
  first writes a **tombstone** (keyed hash of the learner id) into `raw.erased_learners`, then
  deletes the learner's rows from `raw.events` and `raw.dead_letter`. The tombstone makes the
  ingestor refuse events that arrive later from an offline phone (`learner_erased`), and that
  refusal is logged without the learner id or payload. Writing the tombstone first means a crash
  halfway leaves a state where repeating the (idempotent) request finishes the job. dbt models are
  rebuilt from raw on every run, so the learner disappears from every model on the next build;
  `test_erased_learner_disappears_from_every_model_after_rebuild` checks exactly that.
- **Retention (storage limitation).** `levents retention` deletes learning events older than 395
  days (a school year plus a margin) and dead letters older than 30 days. `plan_changed` events
  are kept while the account exists, because they are the history behind `dim_learner` and
  billing records have their own legal retention period.
- **Test.** `test_raw_email_is_never_stored_in_any_table` puts an email in an unexpected field
  of an event that is also rejected, then scans every text and JSON column of every raw table
  for `@`.

## 5. dbt models: `dbt/`

The profile (`dbt/profiles.yml`) has a `duckdb` target (default; path from
`LEVENTS_DUCKDB_PATH`) and a `postgres` target. The models only use SQL and dbt cross-database
macros (`dbt.datediff`, `dbt.dateadd`, `dbt.date_trunc`) that work on both.

| Layer | Model | Grain and purpose |
| --- | --- | --- |
| staging (views) | `stg_lesson_started`, `stg_lesson_completed`, `stg_quiz_answered`, `stg_video_watched`, `stg_plan_changed` | one row per event; JSON fields extracted and typed |
| intermediate | `int_sessions` | one row per learner session |
| marts | `dim_learner` | SCD type 2: one row per learner per plan period |
| marts | `fct_lesson_attempts` | one row per started attempt, with outcome, quiz stats and plan at start |
| marts | `fct_daily_learner_activity` | one row per learner per active day |
| marts | `mart_teacher_weekly` | one row per class per ISO week |

**Sessionization (`int_sessions`).** All learning events of a learner, ordered by event time.
`lag(event_ts)` gives the previous event; a gap of **more than** `session_gap_seconds` (1800)
starts a new session; a running `sum()` of the new-session flags numbers the sessions. A gap of
exactly 30:00 stays in the session, 30:01 starts a new one (both tested). Because this uses event
time, a quiz answer that arrives two days late still lands in the session where it happened.

**SCD2 (`dim_learner`).** Plan changes are ordered per learner; a "change" to the plan the learner
already has (a re-sent webhook) is dropped with `lag(plan)`; `lead(changed_at)` gives `valid_to`
(exclusive); the row with no next change is `is_current`. `fct_lesson_attempts` joins on
`started_at >= valid_from and (started_at < valid_to or valid_to is null)`, a point-in-time join:
an attempt in March counts as "premium" if the learner was premium then, even if they downgraded
in April.

**Teacher mart.** Weeks start on Monday. Enrolled = signed up by the last day of the week.
At risk = enrolled and no learning activity in the 7 days up to the week's last day. With weekly
grain this equals "enrolled but not active this week"; it is computed from the last active date
so the same logic works for a daily as-of date. The HTML report lists who those learners are.

**Tests.** `models/schema.yml` has `unique`, `not_null`, `accepted_values` and `relationships` on
sources and models (39 data tests in total). Two singular tests in `dbt/tests/`:
`assert_no_session_longer_than_4_hours` (a longer session means sessionization or a device clock
is wrong) and `assert_dim_learner_scd2_valid` (exactly one current row per learner, no gaps or
overlaps between versions). One relationship test is `severity: warn` on purpose: a completion
whose start event was malformed has no start, which is a data-quality signal, not a bug.

`tests/test_dbt_models.py` builds the models on a small hand-written scenario where every
expected number is known, and checks that the 4-hour test fails on a 4.8-hour chain of events.

## 6. Synthetic data: `generator.py`

`generate(GeneratorConfig())` builds 6 classes x 24 learners over 8 weeks from one seed
(`random.Random(7)`, including the UUIDs, so two runs are identical). The effects are planted on
purpose and listed in the module docstring: weekday and time-of-day patterns per class type,
geometric drop-out with a much higher hazard in the adult class `AD1`, 2% re-sent duplicates,
1% malformed app events (five kinds), 4% of mobile events delivered 6-72 hours late, and the
app 2.0 rollout that switches `lesson_completed` to v2 from week 5 over two weeks. Plan changes
come from the "billing backend", so they are never malformed or late.

`batches()` groups deliveries the way a client would flush them: what arrived in the same
30-minute window, at most 500 per call. Each batch is posted at its last delivery time.

**Real data.** `levents ingest --file export.jsonl` takes one event per line in the contract
format (the generator's `{"delivered_at", "event"}` lines work too) and sends it through the same
API, so a real export from the app is checked by the same contracts and privacy rules.

## 7. Orchestration

`pipeline.run_all` is the whole loop: generate, save `data/events.jsonl`, ingest through the real
FastAPI app in-process (`TestClient` + simulated clock), close the DuckDB file (DuckDB allows one
writing process, and dbt is another process), `dbt build` in a subprocess, read
`run_results.json`, write the report. A fresh DuckDB file is created on each run so the numbers
are reproducible.

On a schedule, in production:

- **Ingestion is continuous**: `levents serve` (or the `api` container) behind a load balancer.
- **Nightly** (for example 02:00 Europe/Helsinki): `levents dbt-build` then `levents report`,
  so teachers have Monday's report before school starts. `levents retention` once a week.
- Any scheduler works because each step is one idempotent command: cron, a GitHub Actions
  `schedule:` workflow, or a Dagster job with one op per command (`ingest` sensor ->
  `dbt-build` -> `report`). No logic lives in the scheduler, so it can change without touching
  the code.

## 8. Report: `report.py`

`collect()` reads counts from `raw.ingest_batches` and `raw.dead_letter`, rows per model, the
weekly mart and the current at-risk list. `results_markdown()` writes `docs/results.md`;
`plot_weekly()` draws two small multiples (active share and completion rate per class) with the
class that dropped most in blue, and the title sentence is computed from the data.
`teacher_html()` writes one self-contained page per class into the gitignored
`reports/teacher/`, with pseudonymous learner ids only.
