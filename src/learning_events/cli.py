"""`levents` command line: one subcommand per pipeline step, plus `run-all`."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

from .contracts import ContractRegistry, check_registry
from .generator import GeneratorConfig, batches, generate
from .pii import salt_from_env
from .pipeline import (
    DBT_DIR,
    Paths,
    data_label,
    ingest_deliveries,
    load_deliveries,
    load_run_results,
    run_all,
    run_dbt,
    save_deliveries,
)
from .warehouse import connect, warehouse_url


def _config(args: argparse.Namespace) -> GeneratorConfig:
    return GeneratorConfig(
        seed=args.seed, weeks=args.weeks, learners_per_class=args.learners_per_class
    )


def _paths(args: argparse.Namespace) -> Paths:
    return Paths(data_dir=args.data_dir, docs_dir=args.docs_dir, html_dir=args.html_dir)


def cmd_check_contracts(args: argparse.Namespace) -> int:
    registry = ContractRegistry.load()
    report = check_registry(registry)
    for event_type in registry.event_types:
        versions = ", ".join(f"v{v}" for v in registry.versions(event_type))
        print(f"{event_type}: {versions}")
    broken = {pair: problems for pair, problems in report.items() if problems}
    for pair, problems in report.items():
        print(f"{pair}: {'BREAKING' if problems else 'compatible'}")
        for problem in problems:
            print(f"  - {problem}")
    return 1 if broken else 0


def cmd_generate(args: argparse.Namespace) -> int:
    data = generate(_config(args))
    save_deliveries(data, Paths(data_dir=args.data_dir))
    print(f"wrote {len(data.deliveries):,} deliveries to {args.data_dir / 'events.jsonl'}")
    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    deliveries = load_deliveries(args.file or args.data_dir / "events.jsonl")
    if args.url:
        import httpx

        for batch in batches(deliveries):
            response = httpx.post(f"{args.url}/events", json=[d.event for d in batch])
            response.raise_for_status()
        print(f"posted {len(deliveries):,} events to {args.url}")
        return 0
    wh = connect(args.warehouse)
    try:
        wh.init_schema()
        summary = ingest_deliveries(wh, ContractRegistry.load(), salt_from_env(), deliveries)
    finally:
        wh.close()
    print(
        f"accepted {summary.accepted:,}, duplicates {summary.duplicates:,}, "
        f"rejected {sum(summary.rejected.values()):,} {dict(summary.rejected)}"
    )
    return 0


def cmd_dbt_build(args: argparse.Namespace) -> int:
    result = run_dbt(args.warehouse, ["build"], target_path=args.dbt_target)
    print(result.log_tail)
    return result.returncode


def cmd_report(args: argparse.Namespace) -> int:
    from . import report
    from .pipeline import DbtRun

    paths = _paths(args)
    dbt = DbtRun(0, load_run_results(args.dbt_target / "run_results.json"), "")
    wh = connect(args.warehouse)
    try:
        collected = report.collect(wh, dbt, report.load_planted(paths.planted_file))
    finally:
        wh.close()
    outputs = report.write(collected, paths.docs_dir, paths.html_dir, data_label(_config(args)))
    print("\n".join(f"wrote {path}" for path in outputs.values()))
    return 0


def cmd_run_all(args: argparse.Namespace) -> int:
    summary = run_all(
        _config(args), salt_from_env(), args.warehouse, _paths(args), dbt_target=args.dbt_target
    )
    print(
        f"ingested {summary.ingest.accepted:,} events "
        f"({summary.ingest.duplicates:,} duplicates dropped, "
        f"{sum(summary.ingest.rejected.values()):,} rejected); "
        f"dbt: {summary.dbt.count('test', 'pass')} tests passed, "
        f"{summary.dbt.count('test', 'warn')} warned"
    )
    print("\n".join(f"wrote {path}" for path in summary.outputs.values()))
    return 0


def cmd_retention(args: argparse.Namespace) -> int:
    as_of = datetime.fromisoformat(args.as_of) if args.as_of else datetime.now()
    wh = connect(args.warehouse)
    try:
        counts = wh.purge_older_than(
            as_of - timedelta(days=args.event_days), as_of - timedelta(days=args.dead_letter_days)
        )
    finally:
        wh.close()
    print(f"deleted {counts['events']:,} events and {counts['dead_letter']:,} dead letters")
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    from .api import create_app
    from .ingest import Ingestor

    wh = connect(args.warehouse)
    wh.init_schema()
    app = create_app(Ingestor(wh, ContractRegistry.load(), salt_from_env()))
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="levents", description=__doc__)
    parser.add_argument("--warehouse", default=warehouse_url(), help="DuckDB path or Postgres URL")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--docs-dir", type=Path, default=Path("docs"))
    parser.add_argument("--html-dir", type=Path, default=Path("reports/teacher"))
    parser.add_argument("--dbt-target", type=Path, default=DBT_DIR / "target")
    sub = parser.add_subparsers(dest="command", required=True)

    def with_generator_options(p: argparse.ArgumentParser) -> argparse.ArgumentParser:
        p.add_argument("--seed", type=int, default=7)
        p.add_argument("--weeks", type=int, default=8)
        p.add_argument("--learners-per-class", type=int, default=24)
        return p

    sub.add_parser("check-contracts", help="list contracts and check compatibility").set_defaults(
        func=cmd_check_contracts
    )
    with_generator_options(
        sub.add_parser("generate", help="write synthetic deliveries")
    ).set_defaults(func=cmd_generate)
    ingest = sub.add_parser("ingest", help="send data/events.jsonl through the API")
    ingest.add_argument("--url", help="POST to a running API instead of in-process")
    ingest.add_argument("--file", type=Path, help="JSON lines of events (default: generated)")
    ingest.set_defaults(func=cmd_ingest)
    sub.add_parser("dbt-build", help="dbt build (models + tests)").set_defaults(func=cmd_dbt_build)
    with_generator_options(sub.add_parser("report", help="write docs/results.md, chart, HTML"))
    sub.choices["report"].set_defaults(func=cmd_report)
    with_generator_options(sub.add_parser("run-all", help="generate -> ingest -> dbt -> report"))
    sub.choices["run-all"].set_defaults(func=cmd_run_all)
    retention = sub.add_parser("retention", help="delete data older than its retention window")
    retention.add_argument("--event-days", type=int, default=395)
    retention.add_argument("--dead-letter-days", type=int, default=30)
    retention.add_argument("--as-of", help="ISO date to measure from (default: now)")
    retention.set_defaults(func=cmd_retention)
    serve = sub.add_parser("serve", help="run the ingestion API with uvicorn")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.set_defaults(func=cmd_serve)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
