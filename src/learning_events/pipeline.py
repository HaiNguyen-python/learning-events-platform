"""Orchestration: generate -> ingest through the API (in-process) -> dbt build -> report.

Each step is a plain function, so a scheduler (cron, GitHub Actions, Dagster, Airflow) only
has to call `levents run-all` or the single steps; no logic lives in the scheduler.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import warnings
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from .api import create_app
from .contracts import ContractRegistry
from .generator import Delivery, GeneratedData, GeneratorConfig, batches, generate
from .ingest import Ingestor
from .warehouse import Warehouse, connect

with warnings.catch_warnings():
    # Starlette asks for `httpx2`; plain httpx still works and is what FastAPI documents.
    warnings.simplefilter("ignore")
    from fastapi.testclient import TestClient

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"
DUCKDB_ENV = "LEVENTS_DUCKDB_PATH"


class SimulatedClock:
    """Server clock for replays: each batch is 'received' at its simulated arrival time."""

    def __init__(self) -> None:
        self.now = datetime(1970, 1, 1)

    def __call__(self) -> datetime:
        return self.now


@dataclass
class IngestSummary:
    batches: int = 0
    received: int = 0
    accepted: int = 0
    duplicates: int = 0
    rejected: Counter[str] = field(default_factory=Counter)


def ingest_deliveries(
    warehouse: Warehouse, registry: ContractRegistry, salt: str, deliveries: list[Delivery]
) -> IngestSummary:
    """POST every batch to the real FastAPI app, in-process, with a simulated clock."""
    clock = SimulatedClock()
    app = create_app(Ingestor(warehouse, registry, salt), clock=clock)
    summary = IngestSummary()
    with TestClient(app) as client:
        for batch in batches(deliveries):
            clock.now = batch[-1].delivered_at
            response = client.post("/events", json=[d.event for d in batch])
            response.raise_for_status()
            body = response.json()
            summary.batches += 1
            summary.received += body["received"]
            summary.accepted += body["accepted"]
            summary.duplicates += body["duplicates"]
            summary.rejected.update(r["reason"] for r in body["results"] if r["reason"])
    return summary


@dataclass(frozen=True)
class NodeResult:
    unique_id: str
    resource_type: str
    status: str
    failures: int | None


@dataclass(frozen=True)
class DbtRun:
    returncode: int
    results: list[NodeResult]
    log_tail: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    def count(self, resource_type: str, status: str) -> int:
        return sum(r.resource_type == resource_type and r.status == status for r in self.results)


def _dbt_executable() -> str:
    """Prefer the dbt next to the running Python, so a venv's dbt is used even if not on PATH."""
    local = Path(sys.executable).with_name("dbt")
    found = str(local) if local.exists() else shutil.which("dbt")
    if found is None:
        raise RuntimeError("dbt not found; install the package with `pip install -e .`")
    return found


def dbt_env(warehouse: str) -> dict[str, str]:
    """Environment that points dbt's profile at the same warehouse the API wrote to."""
    if warehouse.startswith(("postgresql://", "postgres://")):
        url = urlparse(warehouse)
        return {
            "LEVENTS_DBT_TARGET": "postgres",
            "PGHOST": url.hostname or "localhost",
            "PGPORT": str(url.port or 5432),
            "PGUSER": unquote(url.username or ""),
            "PGPASSWORD": unquote(url.password or ""),
            "PGDATABASE": url.path.lstrip("/"),
        }
    return {"LEVENTS_DBT_TARGET": "duckdb", DUCKDB_ENV: str(Path(warehouse).resolve())}


def run_dbt(
    warehouse: str,
    args: list[str],
    target_path: Path | None = None,
    project_dir: Path = DBT_DIR,
) -> DbtRun:
    """Run dbt in a subprocess and read `run_results.json`.

    A subprocess (not dbt's Python API) keeps dbt's global logging and adapter state out of
    this process and releases the DuckDB file lock as soon as dbt exits.
    """
    target = (target_path or project_dir / "target").resolve()
    env = {**os.environ, **dbt_env(warehouse), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    cmd = [
        _dbt_executable(),
        *args,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
        "--target-path",
        str(target),
        "--log-path",
        str(target / "logs"),
    ]
    results_file = target / "run_results.json"
    results_file.unlink(missing_ok=True)  # never report results from an earlier run
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    return DbtRun(
        proc.returncode, load_run_results(results_file), (proc.stdout + proc.stderr)[-3000:]
    )


def load_run_results(path: Path) -> list[NodeResult]:
    return _parse_results(json.loads(path.read_text())) if path.exists() else []


def _parse_results(run_results: dict[str, Any]) -> list[NodeResult]:
    return [
        NodeResult(
            unique_id=r["unique_id"],
            resource_type=r["unique_id"].split(".")[0],
            status=r["status"],
            failures=r.get("failures"),
        )
        for r in run_results["results"]
    ]


@dataclass(frozen=True)
class Paths:
    data_dir: Path = Path("data")
    docs_dir: Path = Path("docs")
    html_dir: Path = Path("reports/teacher")

    @property
    def events_file(self) -> Path:
        return self.data_dir / "events.jsonl"

    @property
    def planted_file(self) -> Path:
        return self.data_dir / "planted.json"


def save_deliveries(data: GeneratedData, paths: Paths) -> None:
    paths.data_dir.mkdir(parents=True, exist_ok=True)
    with paths.events_file.open("w") as f:
        for d in data.deliveries:
            f.write(json.dumps({"delivered_at": d.delivered_at.isoformat(), "event": d.event}))
            f.write("\n")
    paths.planted_file.write_text(json.dumps(dict(data.planted), indent=2, sort_keys=True))


def load_deliveries(path: Path, now: datetime | None = None) -> list[Delivery]:
    """Read JSON lines: either generator output (`delivered_at` + `event`) or bare events.

    The bare form is the loader for real data: export the app's events (one JSON object per
    line, matching `contracts/`) and run `levents ingest --file export.jsonl`. Real events
    get `now` as their arrival time, like a live API call would.
    """
    arrival = now or datetime.now(UTC).replace(tzinfo=None)
    out = []
    with path.open() as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            if isinstance(row, dict) and set(row) == {"delivered_at", "event"}:
                out.append(Delivery(datetime.fromisoformat(row["delivered_at"]), row["event"]))
            else:
                out.append(Delivery(arrival, row))
    return out


def data_label(cfg: GeneratorConfig) -> str:
    return (
        f"Synthetic data (seed {cfg.seed}, {len(cfg.classes)} classes x "
        f"{cfg.learners_per_class} learners, {cfg.weeks} weeks)"
    )


@dataclass(frozen=True)
class RunSummary:
    ingest: IngestSummary
    dbt: DbtRun
    outputs: dict[str, Path]


def run_all(
    cfg: GeneratorConfig, salt: str, warehouse: str, paths: Paths, dbt_target: Path | None = None
) -> RunSummary:
    """Fresh, reproducible end-to-end run. A DuckDB warehouse file is recreated first."""
    from . import report

    if not warehouse.startswith(("postgresql://", "postgres://")):
        Path(warehouse).unlink(missing_ok=True)
    data = generate(cfg)
    save_deliveries(data, paths)
    wh = connect(warehouse)
    try:
        wh.init_schema()
        ingest = ingest_deliveries(wh, ContractRegistry.load(), salt, data.deliveries)
    finally:
        wh.close()  # DuckDB allows one writing process: release the file for dbt
    dbt = run_dbt(warehouse, ["build"], target_path=dbt_target)
    if not dbt.ok:
        raise RuntimeError(f"dbt build failed:\n{dbt.log_tail}")
    wh = connect(warehouse)
    try:
        collected = report.collect(wh, dbt, dict(data.planted))
    finally:
        wh.close()
    outputs = report.write(collected, paths.docs_dir, paths.html_dir, data_label(cfg))
    return RunSummary(ingest, dbt, outputs)
