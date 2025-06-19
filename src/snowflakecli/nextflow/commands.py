import typer
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflakecli.nextflow.manager import NextflowManager

app = SnowTyperFactory(
    name="nextflow",
    help="Run Nextflow workflows in Snowpark Container Service",
)

@app.command("run", requires_connection=True)
def run_workflow(
    project_dir: str = typer.Argument(
        help="Name of the workflow to run"
    ),
    profile: str = typer.Option(
        None,
        "-profile",
        help="Nextflow profile to use for the workflow execution",
    ),
    **options,
) -> CommandResult:
    """
    Run a Nextflow workflow in Snowpark Container Service.
    """

    manager = NextflowManager(project_dir, profile)
    manager.run()
    # TODO: Implement the actual workflow execution logic
    return MessageResult(f"Executed successfully!")
