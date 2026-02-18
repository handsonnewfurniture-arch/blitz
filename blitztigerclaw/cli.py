import asyncio
import click
from blitztigerclaw.parser import parse_pipeline
from blitztigerclaw.pipeline import Pipeline
from blitztigerclaw.exceptions import BlitzError


@click.group()
@click.version_option(package_name="blitztigerclaw-framework")
def cli():
    """BlitzTigerClaw — 100x efficiency data automation framework.

    Write simple YAML pipelines, get optimized parallel execution.
    """


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--var", "-v", multiple=True, help="Override variable: key=value")
@click.option("--dry-run", is_flag=True, help="Parse and validate only")
@click.option("--verbose", is_flag=True, help="Show detailed execution info")
@click.option("--resume", is_flag=True, help="Resume from last checkpoint")
def run(file, var, dry_run, verbose, resume):
    """Execute a BlitzTigerClaw pipeline from a YAML file."""
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
            click.echo(f"  Checkpoint saved. Resume with: blitztigerclaw run {file} --resume")
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
description: My BlitzTigerClaw pipeline

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
    click.echo(f"Run it: blitztigerclaw run {filename}")


# TPS commands
from blitztigerclaw.cli_tps import metrics, board, lint, queue, work

cli.add_command(metrics)
cli.add_command(board)
cli.add_command(lint)
cli.add_command(queue)
cli.add_command(work)


# ---------------------------------------------------------------------------
# Tiger AI Agent commands
# ---------------------------------------------------------------------------

@cli.group()
def tiger():
    """Tiger AI Agent — autonomous pipeline creation and execution."""


@tiger.command()
def setup():
    """Configure Tiger's Claude API key."""
    from blitztigerclaw.tiger import save_api_key

    api_key = click.prompt(
        "Enter your Anthropic API key",
        hide_input=True,
    )
    if not api_key.startswith("sk-ant-"):
        click.echo("Warning: key doesn't look like an Anthropic API key (expected sk-ant-...)")
        if not click.confirm("Save anyway?"):
            return

    save_api_key(api_key)
    click.echo("API key saved to ~/.blitztigerclaw/tiger_api_key")
    click.echo("Test it: blitztigerclaw tiger run \"list my pipelines\"")


@tiger.command("run")
@click.argument("goal")
def tiger_run(goal):
    """One-shot autonomous execution of a goal.

    Example: blitztigerclaw tiger run "fetch posts from jsonplaceholder and store in posts.db"
    """
    from blitztigerclaw.tiger import TigerAgent

    try:
        agent = TigerAgent()
    except RuntimeError as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1)

    click.echo(f"Tiger is working on: {goal}\n")

    try:
        result = agent.run_goal(goal)
        click.echo(result)
    except Exception as e:
        click.echo(f"\nTiger error: {e}", err=True)
        raise SystemExit(1)


@tiger.command("chat")
def tiger_chat():
    """Interactive conversation with Tiger.

    Type your messages and Tiger will create/run/manage pipelines for you.
    Type 'exit' or 'quit' to end the session.
    """
    from blitztigerclaw.tiger import TigerAgent

    try:
        agent = TigerAgent()
    except RuntimeError as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1)

    click.echo("Tiger AI Agent — Interactive Mode")
    click.echo("Type 'exit' or 'quit' to end.\n")

    while True:
        try:
            message = click.prompt("You", prompt_suffix="> ")
        except (EOFError, KeyboardInterrupt):
            click.echo("\nGoodbye!")
            break

        if message.strip().lower() in ("exit", "quit", "q"):
            click.echo("Goodbye!")
            break

        try:
            response = agent.chat(message)
            click.echo(f"\nTiger> {response}\n")
        except Exception as e:
            click.echo(f"\nTiger error: {e}\n", err=True)


@tiger.command("watch")
@click.option("--interval", "-i", default=60, help="Check interval in seconds")
def tiger_watch(interval):
    """Daemon monitoring mode — watches queue, auto-heals failures.

    Tiger will continuously monitor the KANBAN queue, process pending pipelines,
    and attempt to recover failed pipelines using checkpoints or Claude diagnosis.
    """
    from blitztigerclaw.tiger import TigerAgent

    try:
        agent = TigerAgent()
    except RuntimeError as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1)

    click.echo(f"Tiger Watch Mode — checking every {interval}s")
    click.echo("Press Ctrl+C to stop.\n")

    def _on_event(event: str, details: str = ""):
        ts = __import__("datetime").datetime.now().strftime("%H:%M:%S")
        if details:
            click.echo(f"  [{ts}] {event}: {details}")
        else:
            click.echo(f"  [{ts}] {event}")

    agent.monitor(interval=interval, on_event=_on_event)


if __name__ == "__main__":
    cli()
