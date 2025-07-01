"""
WebSocket client for connecting to Nextflow PTY servers in Snowpark Container Services.

This module provides a WebSocketClient class that handles secure WebSocket connections
with Snowflake authentication for streaming real-time output from Nextflow processes.
"""

import websockets
from websockets.exceptions import ConnectionClosed, InvalidURI, InvalidHandshake
import json
import ssl
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.exceptions import CliError
import typer


class WebSocketClient(SqlExecutionMixin):
    """
    WebSocket client for connecting to Nextflow PTY servers with Snowflake authentication.
    """
    
    def __init__(self):
        super().__init__()
        
    def get_auth_token(self):
        """Get Snowflake session token for authentication"""
        # Access the connection context from SqlExecutionMixin
        ctx = self._conn
        ctx.cursor().execute("alter session set python_connector_query_result_format = 'json'")
        # Get session token using the REST API
        token_data = ctx._rest._token_request('ISSUE')
        return token_data['data']['sessionToken']
        
    async def connect_and_stream(self, server_url: str):
        """Connect to WebSocket server and stream messages"""
        try:
            # Get authentication token
            cc.step("Getting authentication token...")
            token = self.get_auth_token()
            
            # Prepare headers for authentication
            headers = {'Authorization': f'Snowflake Token="{token}"'}
            
            # Create SSL context for wss connection
            ssl_context = ssl.create_default_context()
            
            cc.step(f"Connecting to {server_url}...")
            
            # Connect to the WebSocket server
            async with websockets.connect(
                server_url, 
                additional_headers=headers,
                ssl=ssl_context
            ) as websocket:
                cc.step("Connected! Streaming messages...")
                cc.step("Press Ctrl+C to disconnect")
                cc.step("=" * 50)
                
                try:
                    # Continuously read messages from the server
                    async for message in websocket:
                        try:
                            # Parse JSON message
                            data = json.loads(message)
                            
                            # Handle different message types
                            msg_type = data.get('type', 'unknown')
                            
                            if msg_type == 'output':
                                # Output data from Nextflow
                                print(data.get('data', ''), end='')
                            elif msg_type == 'status':
                                # Status updates
                                status = data.get('status', '')
                                if status == 'starting':
                                    cc.step(f"Starting: {data.get('command', '')}")
                                elif status == 'started':
                                    cc.step(f"Started with PID: {data.get('pid', '')}")
                                elif status == 'completed':
                                    cc.step(f"Completed with exit code: {data.get('exit_code', '')}")
                                elif status == 'error':
                                    cc.step(f"Error: {data.get('error', '')}")
                            else:
                                # Unknown message type, print raw message
                                cc.step(f"Received: {message}")
                                
                        except json.JSONDecodeError:
                            # If not JSON, print raw message
                            print(message)
                            
                except ConnectionClosed:
                    cc.step("Connection closed by server")
                except KeyboardInterrupt:
                    cc.step("Disconnected by user")
                    
        except InvalidURI:
            raise typer.BadParameter(f"Invalid WebSocket URL: {server_url}")
        except InvalidHandshake as e:
            if "401" in str(e):
                raise CliError("Authentication failed. Please check your Snowflake connection.")
            else:
                raise CliError(f"Connection failed: {e}")
        except Exception as e:
            raise CliError(f"Error connecting to server: {e}") 