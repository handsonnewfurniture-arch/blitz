"""TPS CLI commands: metrics, board, lint, queue, work."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime

import click

from blitz.exceptions import BlitzError
from blitz.parser import parse_pipeline
from blitz.pipeline import Pipeline
from blitz.tps.kanban import KanbanBoard
from blitz.tps.linter import PipelineLinter
from blitz.tps.metrics import MetricsStore


@click.command()
@click.option("--pipeline", "-p", default=None, help="Filter by pipeline name")
@click.option("--last", "-n", default=10, help="Show last N runs")
def metrics(pipeline, last):
    """KAIZEN: Show pipeline performance metrics and bottleneck analysis."""

    async def _run():
        store = MetricsStore()

        if pipeline:
            # Show detailed history for one pipeline
            history = await store.get_history(pipeline, limit=last)
            if not history:
                click.echo(f"No runs recorded for '{pipeline}'")
                return

            click.echo(f"\n  KAIZEN Metrics: {pipeline}")
            click.echo(f"  {'=' * 60}")

            for run in history:
                ts = datetime.fromtimestamp(run["started_at"]).strftime("%Y-%m-%d %H:%M")
                status_icon = "OK" if run["status"] == "completed" else "FAIL"
                click.echo(
                    f"  [{status_icon}] {ts}  "
                    f"{run['total_rows']} rows  "
                    f"{run['total_duration_ms']}ms"
                )

            # Bottleneck analysis
            bottlenecks = await store.detect_bottlenecks(pipeline)
            if bottlenecks:
                click.echo(f"\n  Bottleneck Analysis:")
                click.echo(f"  {'-' * 50}")
                for b in bottlenecks:
                    bar = "#" * int(b["pct_of_total"] / 5)
                    click.echo(
                        f"  {b['step_type']:12s} {b['avg_ms']:8.1f}ms "
                        f"({b['pct_of_total']:4.1f}%) {bar}"
                    )
                    if b["suggestion"]:
                        click.echo(f"    -> {b['suggestion']}")
        else:
            # Show summary across all pipelines
            summary = await store.get_all_metrics_summary()
            if not summary:
                click.echo("No pipeline runs recorded yet.")
                click.echo("Run a pipeline first: blitz run pipeline.yaml")
                return

            click.echo(f"\n  KAIZEN Dashboard")
            click.echo(f"  {'=' * 70}")
            click.echo(
                f"  {'Pipeline':<25s} {'Runs':>5s} {'OK':>4s} {'Fail':>4s} "
                f"{'Avg ms':>8s} {'Avg rows':>8s}"
            )
            click.echo(f"  {'-' * 70}")

            for s in summary:
                click.echo(
                    f"  {s['pipeline']:<25s} {s['runs']:>5d} {s['success']:>4d} "
                    f"{s['failed']:>4d} {s['avg_ms']:>8.1f} {s['avg_rows']:>8d}"
                )

    asyncio.run(_run())


@click.command()
def board():
    """KANBAN: View pipeline workflow board."""
    kanban = KanbanBoard()
    board_data = kanban.get_board()

    columns = ["backlog", "in_progress", "done", "failed"]
    headers = {
        "backlog": "BACKLOG",
        "in_progress": "IN PROGRESS",
        "done": "DONE",
        "failed": "FAILED",
    }

    click.echo(f"\n  KANBAN Board")
    click.echo(f"  {'=' * 75}")

    # Header row
    header_line = "  "
    for col in columns:
        header_line += f"{headers[col]:<18s} "
    click.echo(header_line)
    click.echo(f"  {'-' * 75}")

    # Find max items in any column
    max_items = max(len(board_data[col]) for col in columns) if any(board_data.values()) else 0

    if max_items == 0:
        click.echo("  (empty board)")
        click.echo("\n  Add pipelines: blitz queue pipeline.yaml")
        return

    for row_idx in range(max_items):
        line = "  "
        for col in columns:
            items = board_data[col]
            if row_idx < len(items):
                item = items[row_idx]
                name = item.get("pipeline_name", "?")[:16]
                line += f"{name:<18s} "
            else:
                line += f"{'':<18s} "
        click.echo(line)

    click.echo(f"\n  Total: {sum(len(v) for v in board_data.values())} items")


@click.command()
@click.argument("file", type=click.Path(exists=True))
def lint(file):
    """POKA-YOKE: Check pipeline YAML for common mistakes."""
    linter = PipelineLinter()
    results = linter.lint(file)

    if not results:
        click.echo(f"  All clear! No issues found.")
        return

    icons = {"error": "ERR", "warning": "WARN", "suggestion": "TIP"}

    click.echo(f"\n  Pipeline Lint: {file}")
    click.echo(f"  {'=' * 60}")

    for r in results:
        icon = icons.get(r.level, "?")
        step_info = f"step {r.step_index + 1}" if r.step_index is not None else "pipeline"
        click.echo(f"  [{icon}] ({r.tps_principle}) {step_info}: {r.message}")

    errors = sum(1 for r in results if r.level == "error")
    warnings = sum(1 for r in results if r.level == "warning")
    suggestions = sum(1 for r in results if r.level == "suggestion")

    click.echo(f"\n  {errors} errors, {warnings} warnings, {suggestions} suggestions")

    if errors > 0:
        raise SystemExit(1)


@click.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--var", "-v", multiple=True, help="Override variable: key=value")
def queue(file, var):
    """KANBAN: Add a pipeline to the backlog queue."""
    overrides = {}
    for v in var:
        key, _, value = v.partition("=")
        overrides[key] = value

    try:
        definition = parse_pipeline(file, overrides or None)
    except BlitzError as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1)

    kanban = KanbanBoard()
    item_id = kanban.add(
        pipeline_file=file,
        pipeline_name=definition.name,
        variables=overrides,
    )

    click.echo(f"  Queued: {definition.name} (id: {item_id})")
    click.echo(f"  Run next: blitz work")


@click.command()
@click.option("--limit", "-n", default=0, help="Process N items then stop (0=all)")
@click.option("--verbose", is_flag=True, help="Show detailed execution info")
def work(limit, verbose):
    """KANBAN: Pull and execute pipelines from the backlog queue."""
    kanban = KanbanBoard()
    processed = 0

    while True:
        if limit > 0 and processed >= limit:
            break

        item = kanban.pull_next()
        if item is None:
            if processed == 0:
                click.echo("  No items in backlog.")
                click.echo("  Add pipelines: blitz queue pipeline.yaml")
            else:
                click.echo(f"\n  Processed {processed} pipeline(s). Backlog empty.")
            break

        click.echo(
            f"\n  Pulling: {item['pipeline_name']} (id: {item['id']})"
        )

        try:
            overrides = item.get("variables", {})
            definition = parse_pipeline(item["pipeline_file"], overrides or None)
            pipeline = Pipeline(definition, verbose=verbose, kanban_id=item["id"])
            context = asyncio.run(pipeline.run())

            summary = context.summary()
            click.echo(
                f"  Done: {summary['total_rows']} rows in "
                f"{summary['total_duration_ms']:.0f}ms"
            )
        except Exception as e:
            click.echo(f"  Failed: {e}", err=True)

        processed += 1

    if processed > 0:
        click.echo(f"\n  View board: blitz board")
