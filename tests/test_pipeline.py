from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from conftest import SALT, make_event

from learning_events.cli import main
from learning_events.generator import Delivery, GeneratorConfig, batches, generate
from learning_events.pipeline import Paths, RunSummary, dbt_env, run_all, run_dbt
from learning_events.report import MODELS
from learning_events.warehouse import connect

SMALL = GeneratorConfig(seed=3, weeks=3, learners_per_class=5)


def test_generator_is_deterministic_and_seed_sensitive() -> None:
    a, b, c = generate(SMALL), generate(SMALL), generate(GeneratorConfig(seed=4, weeks=3))
    assert [d.event for d in a.deliveries] == [d.event for d in b.deliveries]
    assert a.planted == b.planted
    assert a.deliveries[0].event != c.deliveries[0].event


def test_generator_plants_the_documented_effects(registry) -> None:
    data = generate(GeneratorConfig(seed=7, weeks=8, learners_per_class=10))
    events = [d.event for d in data.deliveries if registry.validate(d.event).ok]
    completed = [e for e in events if e["event_type"] == "lesson_completed"]
    assert {1, 2} <= {e["schema_version"] for e in completed}  # the v2 rollout
    v2_first = min(e["timestamp"] for e in completed if e["schema_version"] == 2)
    assert v2_first >= "2026-03-30"  # not before the release in week 5
    assert data.planted["duplicate"] > 0 and data.planted["late"] > 0
    assert sum(v for k, v in data.planted.items() if k.startswith("malformed:")) > 0
    by_class = {}
    for e in events:
        if e["event_type"] == "lesson_started" and e["timestamp"] >= "2026-04-20":
            by_class[e["actor"]["class_id"]] = by_class.get(e["actor"]["class_id"], 0) + 1
    assert by_class.get("AD1", 0) < min(v for k, v in by_class.items() if k != "AD1")


def test_batches_follow_arrival_windows_and_size() -> None:
    t0 = datetime(2026, 3, 2, 9)
    deliveries = [Delivery(t0 + timedelta(minutes=m), {}) for m in (0, 1, 2, 40, 41, 300)]
    assert [len(b) for b in batches(deliveries, size=2)] == [2, 1, 2, 1]
    assert [len(b) for b in batches(deliveries)] == [3, 2, 1]


def test_dbt_env_targets_postgres_for_a_postgres_url() -> None:
    env = dbt_env("postgresql://u:p%40ss@db:5544/levents")
    assert env["LEVENTS_DBT_TARGET"] == "postgres"
    assert (env["PGHOST"], env["PGPORT"], env["PGPASSWORD"], env["PGDATABASE"]) == (
        "db",
        "5544",
        "p@ss",
        "levents",
    )


@pytest.fixture(scope="module")
def e2e(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[RunSummary, Path, Paths]]:
    root = tmp_path_factory.mktemp("e2e")
    paths = Paths(root / "data", root / "docs", root / "reports")
    warehouse = root / "data" / "wh.duckdb"
    summary = run_all(SMALL, SALT, str(warehouse), paths, dbt_target=root / "target")
    yield summary, warehouse, paths


def test_end_to_end_run_fills_every_model_and_passes_dbt_tests(e2e) -> None:
    summary, warehouse, paths = e2e
    assert summary.dbt.ok and summary.dbt.count("test", "fail") == 0
    assert summary.ingest.duplicates == generate(SMALL).planted["duplicate"]
    wh = connect(warehouse)
    for model in MODELS:
        assert wh.execute(f"SELECT count(*) FROM analytics.{model}")[0][0] > 0, model
    wh.close()
    results = (paths.docs_dir / "results.md").read_text()
    assert "Synthetic data (seed 3" in results and "| Data tests |" in results
    assert (paths.docs_dir / "img" / "weekly_active_and_completion.png").stat().st_size > 10_000
    pages = sorted(p.name for p in paths.html_dir.glob("*.html"))
    assert pages == [
        f"class_{c.class_id}.html" for c in sorted(SMALL.classes, key=lambda c: c.class_id)
    ]


def test_erased_learner_disappears_from_every_model_after_rebuild(e2e, tmp_path: Path) -> None:
    summary, warehouse, paths = e2e
    wh = connect(warehouse)
    learner = "L0001"
    assert wh.erase_learner(learner, "tombstone", datetime(2026, 5, 1))["events"] > 0
    wh.close()
    run = run_dbt(str(warehouse), ["build"], target_path=tmp_path / "target")
    assert run.ok, run.log_tail
    wh = connect(warehouse)
    for model in MODELS:
        if model == "mart_teacher_weekly":
            continue  # aggregated per class; holds no learner ids
        hits = wh.execute(f"SELECT count(*) FROM analytics.{model} WHERE learner_id = ?", [learner])
        assert hits[0][0] == 0, model
    wh.close()


def test_cli_generate_ingest_retention_and_report(tmp_path: Path, e2e) -> None:
    _, warehouse, _ = e2e
    data_dir, db = tmp_path / "data", str(tmp_path / "cli.duckdb")
    common = ["--warehouse", db, "--data-dir", str(data_dir)]
    assert (
        main([*common, "generate", "--seed", "3", "--weeks", "1", "--learners-per-class", "2"]) == 0
    )
    assert (data_dir / "events.jsonl").exists() and (data_dir / "planted.json").exists()
    assert main([*common, "ingest"]) == 0
    assert main([*common, "ingest"]) == 0  # replay: everything is a duplicate now
    wh = connect(db)
    accepted, duplicates = wh.execute(
        "SELECT sum(accepted), sum(duplicates) FROM raw.ingest_batches"
    )[0]
    stored = wh.execute("SELECT count(*) FROM raw.events")[0][0]
    wh.close()
    planted = json.loads((data_dir / "planted.json").read_text())
    assert accepted == stored > 0
    assert duplicates == stored + 2 * planted["duplicate"]  # whole second run = duplicates
    real = tmp_path / "export.jsonl"
    real.write_text(json.dumps(make_event("lesson_started", learner="R1")) + "\n\n")
    assert main([*common, "ingest", "--file", str(real)]) == 0
    wh = connect(db)
    assert wh.execute("SELECT count(*) FROM raw.events WHERE learner_id = 'R1'") == [(1,)]
    wh.close()
    assert main([*common, "retention", "--event-days", "0", "--as-of", "2027-01-01"]) == 0
    wh = connect(db)
    assert wh.execute("SELECT DISTINCT event_type FROM raw.events") == [("plan_changed",)]
    wh.close()

    root = paths_root = e2e[2].data_dir.parent
    out = ["--docs-dir", str(tmp_path / "docs"), "--html-dir", str(tmp_path / "html")]
    report_args = ["--warehouse", str(warehouse), "--data-dir", str(paths_root / "data"), *out]
    assert (
        main(
            [
                *report_args,
                "--dbt-target",
                str(root / "target"),
                "report",
                "--seed",
                "3",
                "--weeks",
                "3",
                "--learners-per-class",
                "5",
            ]
        )
        == 0
    )
    results = (tmp_path / "docs" / "results.md").read_text()
    assert "Planted by the generator" in results and "| Models | 10 |" in results
    assert len(list((tmp_path / "html").glob("*.html"))) == 6
