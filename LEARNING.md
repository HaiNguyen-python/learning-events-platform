# Learning guide (for me, Hai)

Goal: be able to explain every part of this repo in an Analytics Engineer or Data Engineer
interview, change it confidently, and answer "why?" three levels deep. This is the platform I
would want behind HaiEduTech, so I should be able to defend it as a product decision too.

Start with [docs/how-it-works.md](docs/how-it-works.md) (the code walkthrough) and
[docs/decisions.md](docs/decisions.md) (the reasons). This file is the practice plan.

## 1. Run it and watch each step (1 hour)

```bash
make install
export LEVENTS_PII_SALT=$(openssl rand -hex 16)
levents check-contracts                 # lists versions, says v1->v2 is compatible
levents generate                        # data/events.jsonl + data/planted.json
levents ingest                          # accepted / duplicates / rejected by reason
levents ingest                          # again: everything is a duplicate now. Why?
levents dbt-build                       # 10 models, 39 tests, 1 warning: find out what it means
levents report                          # docs/results.md, chart, reports/teacher/*.html
levents serve                           # http://localhost:8000/docs, try POST /events by hand
```

Then open DuckDB and look around: `duckdb data/warehouse.duckdb`, then
`select reason, errors from raw.dead_letter limit 5;` and
`select * from analytics.dim_learner where learner_id in (select learner_id from analytics.dim_learner group by 1 having count(*) > 1) order by learner_id, version;`

## 2. Read the code in this order

| File | What to understand | Question to answer yourself |
| --- | --- | --- |
| `contracts/lesson_completed.v1.json` and `.v2.json` | Envelope, required vs optional, what v2 added | Why is `score_scaled` optional in v2 and not required? |
| `contracts.py` | `validate` order, `flatten`, `breaking_changes` | Why is "required became optional" breaking? |
| `pii.py` | HMAC vs plain SHA-256, `scrub` | How would an attacker reverse an unsalted email hash? |
| `ingest.py` | The per-event decision loop | Why check the tombstone only for *valid* events? |
| `warehouse.py` | One-JSON-parameter bulk insert, `ON CONFLICT`, tombstone first | What happens if the process dies between tombstone and delete? |
| `api.py` | Thin routes, injected clock | Why 200 instead of 400 for a batch with bad events? |
| `generator.py` | Planted effects, batching by arrival window | Why are plan changes never late or malformed? |
| `dbt/models/staging/*.sql` | JSON extraction and typing | Why does one staging model read v1 and v2? |
| `dbt/models/intermediate/int_sessions.sql` | `lag` + flag + running `sum` | What changes if I use `>=` instead of `>`? |
| `dbt/models/marts/dim_learner.sql` | SCD2 with `lag`/`lead` | Why drop "changes" to the same plan? |
| `dbt/models/marts/fct_lesson_attempts.sql` | Point-in-time join | What is wrong with joining on the *current* row? |
| `dbt/models/marts/mart_teacher_weekly.sql` | Spine of class x week, risk logic | Why does at-risk equal enrolled minus active here? |
| `dbt/tests/*.sql` | Singular tests | What other invariant deserves a singular test? |
| `pipeline.py` | Simulated clock, dbt subprocess, DuckDB lock | Why close the warehouse before `dbt build`? |

## 3. Key concepts in my own words (fill these in)

- **Data contract:** ...
- **Schema registry and compatibility modes (BACKWARD, FORWARD, FULL):** ...
- **Idempotency key:** ...
- **Dead-letter queue/table:** ...
- **Event time vs processing (arrival) time:** ...
- **Sessionization:** ...
- **SCD type 1 vs type 2:** ...
- **Point-in-time join:** ...
- **Staging / intermediate / marts layering in dbt:** ...
- **Generic vs singular dbt tests, test severity:** ...
- **Pseudonymisation vs anonymisation (GDPR):** pseudonymised data is still personal data ...
- **Right to erasure, tombstone:** ...

## 4. Interview questions this repo answers

1. *How do you stop a mobile release from breaking your pipeline?* Versioned contracts, the
   compatibility rule, CI gate; show the v1 -> v2 change and that no model changed.
2. *What happens with duplicate events?* `event_id` idempotency key, pre-check plus
   `ON CONFLICT DO NOTHING`; 763 planted, 763 dropped.
3. *How do you handle bad data?* Validate per event, stable reason codes, dead-letter table,
   alert on dead-letter share; the API response tells the client exactly what was wrong.
4. *Late-arriving data?* Model on event time, full rebuild (or incremental with lookback); the
   test where a 2-day-late quiz answer lands in the right session.
5. *How do you build an SCD2 dimension and use it?* `dim_learner` + the point-in-time join in
   `fct_lesson_attempts`.
6. *How do you test dbt models?* Generic tests in `schema.yml`, singular tests, a hand-written
   scenario with known answers, warn vs error severity.
7. *GDPR in a data platform?* Minimisation, keyed-hash pseudonymisation, erasure endpoint with a
   tombstone, retention job, why backups and derived tables matter.
8. *DuckDB or Postgres or Snowflake?* Same SQL through dbt macros; DuckDB for CI speed, Postgres
   for a small production, a cloud warehouse when volume or concurrency needs it.

## 5. Make it mine (do at least three)

- [ ] Add `lesson_completed` v3 that *removes* `success`, watch `levents check-contracts` fail,
      then do it properly: keep v2, add a new optional field instead.
- [ ] Make `fct_lesson_attempts` incremental with a 3-day lookback and prove with a test that a
      late event still updates the right attempt.
- [ ] Add a `dbt source freshness` check on `raw.events` and an alert on dead-letter share > 2%.
- [ ] Export a week of real (consented, test-account) events from HaiEduTech in the contract
      format and run them through `levents ingest --file`.
- [ ] Run `make demo` with Docker and record a short demo of the teacher HTML report.
- [ ] Rewrite the orchestration as a small Dagster job (one op per CLI step) and compare.

## 6. Quality checks I should be able to explain

- `make lint` (ruff), `mypy src` (types), `pytest` (31 test functions, 45 cases, about 97%
  coverage locally), `levents check-contracts`, `pre-commit install`.
- Why the Postgres test is skipped locally and runs in CI (it needs `POSTGRES_TEST_DSN`).
- Why mypy targets Python 3.12 (numpy's newer type stubs use 3.12 syntax).

## 7. Honesty checklist before pushing

- [ ] I can explain every file without looking at it.
- [ ] Every number in the README comes from `docs/results.md` of a run I did myself, and is
      labelled synthetic.
- [ ] I say clearly that AD1's drop-off was planted in the generator, not discovered in real data.
- [ ] Anything I haven't built is under "What I'd do next".

Note: `docker compose` and the Docker image were not run in the environment this repo was first
built in (no Docker daemon there). The Postgres path itself was tested against a local
Postgres 16 server (API backend, dbt-postgres build, report). Run `make demo` once yourself and
fix anything that breaks before you demo it.
