import typer
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.exceptions import CliError
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
    exit_code = manager.run()
    
    if exit_code is not None:
        if exit_code == 0:
            return MessageResult(f"Nextflow workflow completed successfully (exit code: {exit_code})")
        else:
            raise CliError(f"Nextflow workflow completed with exit code: {exit_code}")
    else:
        raise CliError("Nextflow workflow execution interrupted or failed to complete")