import typer
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult, StreamResult
from snowflakecli.nextflow.manager import NextflowManager
from snowflake.cli._plugins.spcs.services.manager import ServiceManager
import itertools
from typing import Generator, Iterable, cast
from snowflake.cli.api.console import cli_console as cc
import asyncio
from snowflakecli.nextflow.util.websocket_client import WebSocketClient

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
    return StreamResult(cast(Generator[CommandResult, None, None], stream))

@app.command("test", requires_connection=True)
def test_websocket_client(
    server: str = typer.Argument(
        help="WebSocket server URL (wss://hostname:port)"
    ),
    **options,
) -> CommandResult:
    """
    Connect to a Nextflow PTY server via WebSocket and stream output.
    """
    
    # Create client and run the connection
    client = WebSocketClient()
    
    # Run the async client
    try:
        asyncio.run(client.connect_and_stream(server))
        return MessageResult("Connection completed successfully")
    except KeyboardInterrupt:
        return MessageResult("Disconnected by user")
    except Exception as e:
        return MessageResult(f"Error: {e}")
