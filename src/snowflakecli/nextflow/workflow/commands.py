import typer
from typing import List, Optional
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import MessageResult, CommandResult, SingleQueryResult
from snowflake.cli._plugins.spcs.services.manager import ServiceManager

app = SnowTyperFactory(
    name="workflow",
    help="Manages nextflow workflows run inside SPCS",
)

def _hello(
) -> CommandResult:
    return MessageResult("foo")


@app.command("submit", requires_connection=True)
def execute_job(
    compute_pool: str = typer.Option(
        ...,
        "--compute-pool",
        help="Compute pool to run the job service on.",
        show_default=False,
    ),
    external_access_integrations: Optional[List[str]] = typer.Option(
        None,
        "--eai-name",
        help="Identifies External Access Integrations(EAI) that the job service can access. This option may be specified multiple times for multiple EAIs.",
    ),
    **options,
) -> CommandResult:
    """
    Creates and executes a job service in the current schema.
    """
    cursor = ServiceManager().execute_job(
        job_service_name=
        compute_pool=compute_pool,
        spec_path=spec_path,
        external_access_integrations=external_access_integrations,
        query_warehouse=None,
        comment=comment,
    )
    return SingleQueryResult(cursor)