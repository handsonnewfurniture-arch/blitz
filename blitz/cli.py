import asyncio
import click
from blitz.parser import parse_pipeline
from blitz.pipeline import Pipeline
from blitz.exceptions import BlitzError


@click.group()
@click.version_option(package_name="blitz-framework")
def cli():
    """Blitz — 100x efficiency data automation framework.

    Write simple YAML pipelines, get optimized parallel execution.
    """


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--var", "-v", multiple=True, help="Override variable: key=value")
@click.option("--dry-run", is_flag=True, help="Parse and validate only")
@click.option("--verbose", is_flag=True, help="Show detailed execution info")
@click.option("--resume", is_flag=True, help="Resume from last checkpoint")
def run(file, var, dry_run, verbose, resume):
    """Execute a Blitz pipeline from a YAML file."""
    overrides = {}
    for v in var:
        key, _, value = v.partition("=")
        overrides[key] = value

    try:
        definition = parse_pipeline(file, overrides or None)
    except BlitzError as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1)

    if dry_run:
        click.echo(f"Pipeline: {definition.name}")
        if definition.description:
            click.echo(f"  {definition.description}")
        click.echo(f"Steps ({len(definition.steps)}):")
        for i, step in enumerate(definition.steps):
            click.echo(f"  {i + 1}. {step.step_type}")
        if definition.checkpoint:
            click.echo(f"Checkpoint: enabled")
        click.echo("Validation: OK")
        return

    click.echo(f"Running pipeline: {definition.name}")
    if verbose:
        click.echo(f"Steps: {len(definition.steps)}")
        if definition.checkpoint:
            click.echo(f"Checkpoint: enabled")

    pipeline = Pipeline(definition, verbose=verbose, resume=resume)

    try:
        context = asyncio.run(pipeline.run())
    except BlitzError as e:
        click.echo(f"\nError: {e}", err=True)
        if definition.checkpoint:
            click.echo(f"  Checkpoint saved. Resume with: blitz run {file} --resume")
        raise SystemExit(1)

    summary = context.summary()
    click.echo(f"\n--- Pipeline Complete ---")
    click.echo(f"Rows: {summary['total_rows']}")
    click.echo(f"Duration: {summary['total_duration_ms']:.0f}ms")
    if summary.get("memory_peak_mb", 0) > 0:
        click.echo(f"Memory peak: {summary['memory_peak_mb']:.1f}MB")
    if summary.get("streaming_mode"):
        click.echo(f"Mode: streaming")
    for s in summary["steps"]:
        errors_str = f" ({s['errors']} errors)" if s["errors"] else ""
        click.echo(f"  {s['type']}: {s['rows']} rows in {s['ms']}ms{errors_str}")
    if summary.get("jit_steps_skipped"):
        click.echo(f"  JIT: {summary['jit_steps_skipped']} steps skipped (unchanged)")
    click.echo(f"  [KAIZEN: metrics recorded]")


@cli.command()
@click.argument("file", type=click.Path(exists=True))
def validate(file):
    """Validate a pipeline YAML file without executing."""
    try:
        definition = parse_pipeline(file)
    except BlitzError as e:
        click.echo(f"Invalid: {e}", err=True)
        raise SystemExit(1)

    click.echo(f"Valid pipeline: {definition.name}")
    click.echo(f"Steps: {len(definition.steps)}")
    for i, step in enumerate(definition.steps):
        click.echo(f"  {i + 1}. {step.step_type}")


@cli.command()
@click.option("--name", "-n", default="my_pipeline", help="Pipeline name")
def init(name):
    """Create a starter pipeline.yaml in the current directory."""
    template = f"""name: {name}
description: My Blitz pipeline

steps:
  # Step 1: Read input data
  - file:
      action: read
      path: input.json
      format: json

  # Step 2: Transform the data
  - transform:
      select: [id, name, value]
      filter: "value > 0"
      sort: "value desc"

  # Step 3: Output results
  - load:
      target: stdout
"""
    filename = "pipeline.yaml"
    with open(filename, "w") as f:
        f.write(template)
    click.echo(f"Created {filename}")
    click.echo(f"Run it: blitz run {filename}")


# TPS commands
from blitz.cli_tps import metrics, board, lint, queue, work

cli.add_command(metrics)
cli.add_command(board)
cli.add_command(lint)
cli.add_command(queue)
cli.add_command(work)


if __name__ == "__main__":
    cli()
