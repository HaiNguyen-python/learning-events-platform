"""Results report: docs/results.md, one chart, and a static HTML page per class for teachers.

Every number comes from the warehouse and dbt's run_results.json of the last build, so the
report stays true when it is rerun on other (or real) data.
"""

from __future__ import annotations

import html
import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from .pipeline import DbtRun
from .warehouse import Warehouse

SURFACE, INK, INK_2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
HIGHLIGHT, CONTEXT = "#2a78d6", "#b7b7b2"
MODELS = (
    "stg_lesson_started",
    "stg_lesson_completed",
    "stg_quiz_answered",
    "stg_video_watched",
    "stg_plan_changed",
    "int_sessions",
    "dim_learner",
    "fct_lesson_attempts",
    "fct_daily_learner_activity",
    "mart_teacher_weekly",
)


# Why a known warning is expected, keyed by the start of the test name.
_WARN_NOTES = {
    "relationships_stg_lesson_completed": (
        " These are completions whose `lesson_started` event was malformed and dead-lettered,"
        " so the attempt has no start. `fct_lesson_attempts` is driven by starts and skips them."
    )
}


@dataclass
class WeekRow:
    class_id: str
    week_start: date
    enrolled: int
    active: int
    started: int
    completed: int
    completion_rate: float | None
    avg_quiz_score: float | None
    at_risk: int


@dataclass
class ReportData:
    ingest: dict[str, int]
    rejected_by_reason: list[tuple[str, int]]
    contract_versions: list[tuple[str, int, int]]
    late_events: int
    model_rows: dict[str, int]
    weekly: list[WeekRow]
    at_risk_now: dict[str, list[tuple[str, date | None, int | None]]]
    dbt: DbtRun
    planted: dict[str, int] = field(default_factory=dict)


def collect(wh: Warehouse, dbt: DbtRun, planted: dict[str, int] | None = None) -> ReportData:
    totals = wh.execute(
        "SELECT count(*), coalesce(sum(received), 0), coalesce(sum(accepted), 0), "
        "coalesce(sum(duplicates), 0), coalesce(sum(rejected), 0) FROM raw.ingest_batches"
    )[0]
    ingest = dict(
        zip(
            ("batches", "received", "accepted", "duplicates", "rejected"),
            map(int, totals),
            strict=True,
        )
    )
    reasons = wh.execute(
        "SELECT reason, count(*) FROM raw.dead_letter GROUP BY reason ORDER BY 2 DESC, 1"
    )
    versions = wh.execute(
        "SELECT event_type, schema_version, count(*) FROM raw.events "
        "GROUP BY event_type, schema_version ORDER BY 1, 2"
    )
    late = wh.execute(
        "SELECT count(*) FROM raw.events WHERE received_at > event_ts + INTERVAL '2 hours'"
    )[0][0]
    rows = {m: int(wh.execute(f"SELECT count(*) FROM analytics.{m}")[0][0]) for m in MODELS}
    weekly = [
        WeekRow(
            str(r[0]),
            r[1],
            int(r[2]),
            int(r[3]),
            int(r[4]),
            int(r[5]),
            None if r[6] is None else float(r[6]),
            None if r[7] is None else float(r[7]),
            int(r[8]),
        )
        for r in wh.execute(
            "SELECT class_id, week_start, enrolled_learners, active_learners, lessons_started, "
            "lessons_completed, completion_rate, avg_quiz_score, learners_at_risk "
            "FROM analytics.mart_teacher_weekly ORDER BY class_id, week_start"
        )
    ]
    return ReportData(
        ingest=ingest,
        rejected_by_reason=[(str(r), int(n)) for r, n in reasons],
        contract_versions=[(str(t), int(v), int(n)) for t, v, n in versions],
        late_events=int(late),
        model_rows=rows,
        weekly=weekly,
        at_risk_now=_at_risk_now(wh),
        dbt=dbt,
        planted=planted or {},
    )


def _at_risk_now(wh: Warehouse) -> dict[str, list[tuple[str, date | None, int | None]]]:
    """Learners with no activity in the last 7 days of data, per class, longest-idle first."""
    rows = wh.execute(
        """
        WITH last_day AS (SELECT max(activity_date) AS d FROM analytics.fct_daily_learner_activity),
        learners AS (
            SELECT learner_id, min(class_id) AS class_id FROM analytics.dim_learner GROUP BY 1),
        seen AS (
            SELECT learner_id, max(activity_date) AS last_active
            FROM analytics.fct_daily_learner_activity GROUP BY 1)
        SELECT l.class_id, l.learner_id, s.last_active,
               CAST(ld.d - s.last_active AS INTEGER) AS days_idle
        FROM learners l CROSS JOIN last_day ld LEFT JOIN seen s USING (learner_id)
        WHERE s.last_active IS NULL OR ld.d - s.last_active >= 7
        ORDER BY l.class_id, s.last_active NULLS FIRST, l.learner_id
        """
    )
    out: dict[str, list[tuple[str, date | None, int | None]]] = {}
    for class_id, learner_id, last_active, days in rows:
        out.setdefault(str(class_id), []).append((str(learner_id), last_active, days))
    return out


@dataclass(frozen=True)
class ClassSummary:
    class_id: str
    first_active: int
    last_active: int
    completion_rate: float
    avg_quiz_score: float
    at_risk_last_week: int

    @property
    def active_change(self) -> float:
        return self.last_active / self.first_active - 1 if self.first_active else 0.0


def summarise(weekly: list[WeekRow]) -> list[ClassSummary]:
    out = []
    for class_id in sorted({w.class_id for w in weekly}):
        weeks = [w for w in weekly if w.class_id == class_id]
        started = sum(w.started for w in weeks)
        scores = [w.avg_quiz_score for w in weeks if w.avg_quiz_score is not None]
        out.append(
            ClassSummary(
                class_id,
                weeks[0].active,
                weeks[-1].active,
                sum(w.completed for w in weeks) / started if started else 0.0,
                sum(scores) / len(scores) if scores else 0.0,
                weeks[-1].at_risk,
            )
        )
    return out


def headline(summaries: list[ClassSummary]) -> tuple[str, str, float, float]:
    """(worst class, sentence, its change, mean change of the others), from the data."""
    worst = min(summaries, key=lambda s: s.active_change)
    others = [s.active_change for s in summaries if s is not worst]
    mean_others = sum(others) / len(others) if others else 0.0
    sentence = (
        f"Class {worst.class_id} lost {-worst.active_change:.0%} of its weekly active learners "
        f"between the first and last week; the other classes lost {-mean_others:.0%} on average"
    )
    return worst.class_id, sentence, worst.active_change, mean_others


def _nan(value: float | None) -> float:
    return float("nan") if value is None else value


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1%}"


def results_markdown(data: ReportData, data_label: str) -> str:
    ing, dbt = data.ingest, data.dbt
    lines = [
        "# Results",
        "",
        f"Data: **{data_label}**.",
        "",
        "Produced by `levents report` from the warehouse and the last `dbt build`;",
        "rerun `levents run-all` to regenerate everything.",
        "",
        "## Ingestion",
        "",
        "| Step | Events |",
        "| --- | --- |",
        f"| Received by `POST /events` ({ing['batches']} batches) | {ing['received']:,} |",
        f"| Accepted into `raw.events` | {ing['accepted']:,} |",
        f"| Duplicates dropped (same `event_id`) | {ing['duplicates']:,} |",
        f"| Rejected to `raw.dead_letter` | {ing['rejected']:,} |",
        f"| Accepted events that arrived more than 2 hours late | {data.late_events:,} |",
        "",
        "| Dead-letter reason | Events |",
        "| --- | --- |",
        *[f"| `{reason}` | {n:,} |" for reason, n in data.rejected_by_reason],
    ]
    if data.planted:
        malformed = sum(v for k, v in data.planted.items() if k.startswith("malformed:"))
        lines += [
            "",
            "Planted by the generator vs. found by the pipeline:",
            "",
            "| Planted problem | Planted | Found |",
            "| --- | --- | --- |",
            f"| Re-sent duplicates | {data.planted.get('duplicate', 0):,} "
            f"| {ing['duplicates']:,} dropped |",
            f"| Malformed events | {malformed:,} | {ing['rejected']:,} rejected |",
            f"| Late mobile events (6-72 h) | {data.planted.get('late', 0):,} "
            f"({data.planted.get('late_and_malformed', 0)} of them also malformed) "
            f"| {data.late_events:,} late in `raw.events` |",
        ]
    lines += [
        "",
        "## Contract versions in raw.events",
        "",
        "| Event type | Version | Events |",
        "| --- | --- | --- |",
        *[f"| {t} | v{v} | {n:,} |" for t, v, n in data.contract_versions],
        "",
        "## dbt models (rows)",
        "",
        "| Model | Rows |",
        "| --- | --- |",
        *[f"| `{m}` | {n:,} |" for m, n in data.model_rows.items()],
        "",
        "## dbt build",
        "",
        "| Node type | Pass / OK | Warn | Fail / Error |",
        "| --- | --- | --- | --- |",
        f"| Models | {dbt.count('model', 'success')} | - | {dbt.count('model', 'error')} |",
        f"| Data tests | {dbt.count('test', 'pass')} | {dbt.count('test', 'warn')} "
        f"| {dbt.count('test', 'fail') + dbt.count('test', 'error')} |",
    ]
    warned = [r for r in dbt.results if r.status == "warn"]
    for r in warned:
        lines.append(
            f"\nWarning: `{r.unique_id.split('.')[-2]}` found {r.failures} rows (severity warn)."
            + _WARN_NOTES.get(r.unique_id.split(".")[-2].split("_attempt_id")[0], "")
        )
    summaries = summarise(data.weekly)
    if summaries:
        weeks = len({w.week_start for w in data.weekly})
        lines += [
            "",
            "## Teacher view (mart_teacher_weekly)",
            "",
            f"**{headline(summaries)[1]}.**",
            "",
            f"| Class | Active learners week 1 -> week {weeks} | Change | Lesson completion "
            "| Avg quiz score | At risk in last week |",
            "| --- | --- | --- | --- | --- | --- |",
            *[
                f"| {s.class_id} | {s.first_active} -> {s.last_active} | {s.active_change:+.0%} "
                f"| {s.completion_rate:.1%} | {s.avg_quiz_score:.1%} | {s.at_risk_last_week} |"
                for s in summaries
            ],
        ]
    return "\n".join(lines) + "\n"


def _style(ax: Any, title: str) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(colors=INK_2, labelsize=9, length=0)
    ax.set_title(title, loc="left", fontsize=10.5, color=INK, pad=8)


def plot_weekly(weekly: list[WeekRow], path: Path, data_label: str) -> None:
    """Two small multiples: active share and completion rate per class; worst class in blue."""
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    summaries = summarise(weekly)
    worst, sentence, _, _ = headline(summaries)
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.2), dpi=150)
    fig.set_facecolor(SURFACE)
    panels = (
        ("Weekly active learners, % of enrolled", lambda w: w.active / w.enrolled),
        ("Lesson completion rate", lambda w: _nan(w.completion_rate)),
    )
    classes = sorted({w.class_id for w in weekly}, key=lambda c: c == worst)  # worst on top
    for ax, (title, metric) in zip(axes, panels, strict=True):
        for class_id in classes:
            rows = [w for w in weekly if w.class_id == class_id]
            xs = list(range(1, len(rows) + 1))
            ys = [metric(w) for w in rows]
            is_worst = class_id == worst
            ax.plot(
                xs,
                ys,
                color=HIGHLIGHT if is_worst else CONTEXT,
                linewidth=2.2 if is_worst else 1.4,
                marker="o" if is_worst else None,
                markersize=4,
                zorder=3 if is_worst else 2,
            )
            if is_worst:  # grey lines overlap, so only the highlighted class is labelled
                ax.annotate(
                    class_id,
                    (xs[-1], ys[-1]),
                    xytext=(5, 0),
                    textcoords="offset points",
                    va="center",
                    fontsize=9,
                    color=INK,
                    fontweight="bold",
                )
        _style(ax, title)
        ax.set_ylim(0, 1.05)
        ax.yaxis.set_major_formatter(PercentFormatter(1.0, decimals=0))
        ax.set_xlabel("Week of term", color=INK_2, fontsize=9)
        ax.set_xticks(range(1, len({w.week_start for w in weekly}) + 1))
    fig.suptitle(
        sentence.replace("; ", ";\n"),
        x=0.01,
        y=0.98,
        ha="left",
        fontsize=12,
        color=INK,
        fontweight="bold",
    )
    others = ", ".join(sorted(c for c in classes if c != worst))
    fig.text(
        0.01,
        0.855,
        f"Blue: {worst}. Grey: {others}. {data_label}.",
        fontsize=9,
        color=INK_2,
    )
    fig.subplots_adjust(left=0.06, right=0.95, top=0.74, bottom=0.13, wspace=0.22)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


_HTML_STYLE = """
body{font-family:system-ui,sans-serif;margin:2rem auto;max-width:820px;padding:0 16px;
color:#0b0b0b;background:#fcfcfb}
table{border-collapse:collapse;width:100%;margin:1rem 0}
th,td{text-align:right;padding:6px 10px;border-bottom:1px solid #e4e3df}
th:first-child,td:first-child{text-align:left}
.muted{color:#52514e}
"""


def teacher_html(class_id: str, weekly: list[WeekRow], at_risk: list[Any], label: str) -> str:
    """A self-contained page a teacher can open without any server or login."""
    esc = html.escape
    rows = "\n".join(
        f"<tr><td>{w.week_start}</td><td>{w.active} / {w.enrolled}</td>"
        f"<td>{w.completed} / {w.started}</td><td>{_pct(w.completion_rate)}</td>"
        f"<td>{_pct(w.avg_quiz_score)}</td><td>{w.at_risk}</td></tr>"
        for w in weekly
    )
    risk_rows = (
        "\n".join(
            f"<tr><td>{esc(lid)}</td><td>{last or 'never'}</td>"
            f"<td>{'-' if days is None else days}</td></tr>"
            for lid, last, days in at_risk
        )
        or '<tr><td colspan="3">Nobody: every learner was active in the last 7 days.</td></tr>'
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Class {esc(class_id)} weekly report</title><style>{_HTML_STYLE}</style></head>
<body>
<h1>Class {esc(class_id)}: weekly learning report</h1>
<p class="muted">{esc(label)}. Learners are shown by pseudonymous id only.</p>
<h2>Week by week</h2>
<table><thead><tr><th>Week starting</th><th>Active / enrolled</th><th>Lessons done / started</th>
<th>Completion</th><th>Avg quiz score</th><th>At risk</th></tr></thead>
<tbody>
{rows}
</tbody></table>
<h2>Learners to contact (no activity in the last 7 days)</h2>
<table><thead><tr><th>Learner</th><th>Last active</th><th>Days inactive</th></tr></thead>
<tbody>
{risk_rows}
</tbody></table>
</body></html>
"""


def write(data: ReportData, docs_dir: Path, html_dir: Path, data_label: str) -> dict[str, Path]:
    docs_dir.mkdir(parents=True, exist_ok=True)
    results = docs_dir / "results.md"
    results.write_text(results_markdown(data, data_label))
    chart = docs_dir / "img" / "weekly_active_and_completion.png"
    plot_weekly(data.weekly, chart, data_label)
    html_dir.mkdir(parents=True, exist_ok=True)
    for class_id in sorted({w.class_id for w in data.weekly}):
        weeks = [w for w in data.weekly if w.class_id == class_id]
        page = teacher_html(class_id, weeks, data.at_risk_now.get(class_id, []), data_label)
        (html_dir / f"class_{class_id}.html").write_text(page)
    return {"results": results, "chart": chart, "html": html_dir}


def load_planted(path: Path) -> dict[str, int]:
    return json.loads(path.read_text()) if path.exists() else {}
