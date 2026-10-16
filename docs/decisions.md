# Design decisions

Short records of the main choices: what was decided, why, and what it costs. Format inspired by
Architecture Decision Records (ADRs).

## 1. Contracts as JSON Schema files in the repo, not a schema registry service

- **Decision:** one JSON Schema per event type and version in `contracts/`, a compatibility
  checker in Python, enforced by a pytest test and a CI step.
- **Why:** the app and the pipeline live in one organisation; a pull request review plus a CI
  gate gives the same protection as a registry with nothing extra to run. JSON Schema is what the
  web and mobile teams already know.
- **Cost:** producers cannot fetch schemas at runtime, and nothing stops an app build from
  sending a version that was never merged (it is dead-lettered as `unsupported_schema_version`).
- **Alternatives:** Confluent Schema Registry with Avro or Protobuf (better once events go
  through Kafka), a contracts package published to the app teams.

## 2. A stricter compatibility rule than Avro BACKWARD

- **Decision:** adding optional fields is the only safe change to required fields; removing,
  renaming, retyping or relaxing a required field is breaking, and so is a new required field.
- **Why:** both sides must keep working: old app builds keep sending the old version for weeks
  (producers), and the dbt models select required fields directly (consumers).
- **Cost:** some harmless changes (widening `integer` to `number`) are flagged; they need a new
  event version instead of an edit.

## 3. Per-event validation, 200 with per-event results

- **Decision:** `POST /events` never rejects a whole batch for one bad event.
- **Why:** mobile clients batch offline events. If one broken event failed the call, the client
  would resend all of them forever, or drop the good ones.
- **Cost:** clients must read the response to know what failed. The reason codes are stable, so
  they can.

## 4. Idempotency by client `event_id`, twice

- **Decision:** a pre-check query for honest counts, plus `ON CONFLICT DO NOTHING` on the primary
  key for safety.
- **Why:** retries after timeouts are normal on mobile networks (2% in the synthetic data). The
  pre-check alone has a race between two workers; the constraint alone cannot tell the client
  which events were duplicates.
- **Cost:** a client that reuses an `event_id` for a different event loses the second one. That
  is a client bug and shows up as a duplicate count that does not match retries.

## 5. Pseudonymise at the edge with a keyed hash

- **Decision:** HMAC-SHA256 with a secret salt from the environment; the plain email never
  reaches a table, including the dead-letter table. No default salt.
- **Why:** a plain SHA-256 of an email can be reversed by hashing a list of candidate emails.
  Scrubbing the whole payload covers emails in unexpected places (malformed events, free text).
- **Cost:** losing the salt means new events no longer join to old hashes; rotating it needs a
  re-hash of `email_hash`. The salt belongs in a secret manager, not in `.env` files.

## 6. Erasure with a tombstone, and models rebuilt from raw

- **Decision:** `DELETE /learners/{id}` writes a tombstone, deletes raw rows, and relies on the
  next `dbt build` to remove the learner from derived tables.
- **Why:** offline phones can send a deleted learner's events days later; the tombstone refuses
  them. Rebuilding from raw means there is exactly one place to delete from.
- **Cost:** derived tables keep the learner until the next build (at most a day on a nightly
  schedule; GDPR asks for "without undue delay", usually read as within a month). Backups need
  their own expiry. A dead letter whose `learner_id` was unreadable cannot be found by id; the
  30-day dead-letter retention covers it.

## 7. Full-refresh dbt models on event time

- **Decision:** every model is a view or a full table rebuild, and all logic uses event time.
- **Why:** late events (4% of mobile traffic here) and deletions are handled for free, and the
  data volume (about 40,000 events per term per 150 learners) rebuilds in seconds.
- **Cost:** does not scale to billions of events. The next step is incremental models with a
  lookback window (for example `event_ts > max(event_ts) - interval '3 days'`) and a periodic full
  refresh.

## 8. DuckDB for local, tests and CI; Postgres for docker-compose

- **Decision:** one `Warehouse` class over both engines; dbt models written with cross-database
  macros; CI runs dbt on DuckDB and runs the Postgres backend test against a Postgres service.
- **Why:** DuckDB needs no server, so `pytest` and `levents run-all` work on any laptop and CI stays
  fast. Postgres is what a small team would run in production.
- **Cost:** two engines to keep in sync. The dbt models were run on Postgres 16 with
  `dbt-postgres` by hand when the repo was built (same results as DuckDB); CI does not repeat
  that.

## 9. dbt in a subprocess, results from `run_results.json`

- **Decision:** `pipeline.run_dbt` runs the `dbt` executable and parses `run_results.json`.
- **Why:** dbt's Python API shares global logging and adapter state with the caller, and a DuckDB
  file stays locked while any connection in the process is open. A subprocess ends cleanly. The
  report's test counts come from dbt's own artifact, not from parsing log lines.
- **Cost:** about five seconds of dbt start-up per call.

## 10. In-process API ingest for the pipeline

- **Decision:** `run-all` posts every batch through the real FastAPI app with `TestClient` and a
  simulated clock.
- **Why:** the demo and the tests exercise the same request path as production (JSON
  serialisation, validation, response model) without starting a server.
- **Cost:** about 1,500 requests take most of the two-minute run time; a direct call to
  `Ingestor` would be faster but would skip the API.

## 11. Synthetic data with planted, documented effects

- **Decision:** a seeded generator; the effects the report finds are listed in its docstring.
- **Why:** there is no public dataset of classroom app events with emails, plan changes and
  realistic mess, and I cannot publish real learner data. Planted effects make the pipeline
  testable: the report must find the duplicates, broken events and the struggling class.
- **Cost:** synthetic learners are tidier than real ones. All numbers are labelled synthetic.
