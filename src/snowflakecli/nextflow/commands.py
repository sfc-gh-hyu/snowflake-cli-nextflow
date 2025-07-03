import typer
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.exceptions import CliError
from snowflakecli.nextflow.manager import NextflowManager
from snowflakecli.nextflow.config.commands import app as config_app
from snowflake.cli.api.plugins.plugin_config import PluginConfigProvider

app = SnowTyperFactory(
    name="nextflow",
    help="Run Nextflow workflows in Snowpark Container Service",
)

app.add_typer(config_app)

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

    plugin_config = PluginConfigProvider.get_config("nextflow")
    nf_snowflake_image = plugin_config.internal_config.get("nf_snowflake_image")
    if nf_snowflake_image is None:
        raise CliError("nf_snowflake_image is not set in the plugin config. Please run `snow nextflow config set -key nf_snowflake_image -value <image>` to set the image.")

    manager = NextflowManager(project_dir, profile, nf_snowflake_image)
    exit_code = manager.run()
    
    if exit_code is not None:
        if exit_code == 0:
            return MessageResult(f"Nextflow workflow completed successfully (exit code: {exit_code})")
        else:
            raise CliError(f"Nextflow workflow completed with exit code: {exit_code}")
    else:
        raise CliError("Nextflow workflow execution interrupted or failed to complete")