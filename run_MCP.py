import os

from dotenv import load_dotenv

# load_dotenv(os.getenv("APP_CONFIG_PATH"))
# load_dotenv(os.getenv("VAULT_CONFIG_PATH"))

# APP_HOST_IP = os.getenv("APP_HOST_IP")
load_dotenv()
PREFIX_URL = os.getenv("PREFIX_URL")

from starlette.applications import Starlette
from mcp.server.sse import SseServerTransport
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Mount, Route
import uvicorn
import asyncio

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from proxy_server import create_proxy_server
from datetime import datetime

from database.mongodb import AsyncMongoConnection

from log.logWrapper import get_logger
clog = get_logger(__name__)


async def check_liveness(request: Request):
    output_json = {"output": "success"}
    output_json.update({"message": f"queryengine service is live on {datetime.now()}"})
    return JSONResponse(content=output_json, status_code=200)


async def check_readiness(request: Request):
    output_json = {"output": "success"}
    output_json.update({"message": f"queryengine service is ready on {datetime.now()}"})
    return JSONResponse(content=output_json, status_code=200)


def get_secret(key, alias_keys: list, secrets: dict):

    value = secrets.get(key)
    if value:
        return value

    for alias_key in alias_keys:
        value = secrets.get(alias_key)
        if value:
            return value

    return None


async def get_runtime_args_and_envs(
    connection_details: list,
    database,
    runtime_args_info: list[dict],
    envs_info: list[dict],
):
    if not (runtime_args_info or envs_info):
        return [], {}, []

    tool_name = connection_details[0]
    user_id = None
    if len(connection_details) > 1:
        user_id = connection_details[1]

    mcp_cred_collection = database.get_collection("mcp_credentials")
    filter_conditions = [{"tool_name": tool_name}]
    if user_id:
        filter_conditions.append({"user_id": user_id})

    credentials = await mcp_cred_collection.find_one(filter={"$and": filter_conditions})
    secrets = credentials.get("secrets", {})

    runtime_args = []
    for arg in runtime_args_info:
        arg_name = arg.get("name")
        arg_value = get_secret(arg_name, arg.get("alias", []), secrets)
        if arg_value:
            runtime_args.append(f"--{arg_name}={arg_value}")

    envs = {}
    env_args = []
    for env in envs_info:
        env_name = env.get("name")
        env_value = get_secret(env_name, env.get("alias", []), secrets)
        if env_value:
            envs[env_name] = env_value
            env_args.append("-e")
            env_args.append(f"{env_name}={env_value}")

    return runtime_args, envs, env_args


async def validate_configurations(connector_split, configuration, database):

    if configuration.get("transport") == "stdio":
        args = configuration.get("args", [])
        runtime_args_info = configuration.get("runtime_args", [])
        envs_info = configuration.get("env", [])

        runtime_args, envs, env_args = await get_runtime_args_and_envs(
            connector_split, database, runtime_args_info, envs_info
        )

        for env in env_args:
            args.insert(-1, env)

        args.extend(runtime_args)

        return StdioServerParameters(
            command=configuration.get("command", ""), args=args, env=envs
        )

    return None


async def fetch_connector_details(connector_id: str):

    try:
        database = await AsyncMongoConnection().get_connection(db_name="genai_studio")
        connector_split = connector_id.split("-", 2)
        collection = database.get_collection("mcp_tool_configuration")
        tool_config = await collection.find_one(filter={"name": connector_split[0]})

        if not tool_config:
            raise ValueError(f"Invalid connector id: {connector_id}")

        config = await validate_configurations(
            connector_split, tool_config.get("configurations", {}), database=database
        )

        if not config:
            raise ValueError(f"Invalid connector id: {connector_id}")
        return config
    except Exception as err:
        clog.info(str(err))


async def monitor_disconnect(request: Request):
    """Monitors the request connection and returns when disconnected."""
    while True:
        if await request.is_disconnected():
            clog.info("Client disconnected detected by monitor.")
            return
        # Check connection status periodically
        await asyncio.sleep(1)


def create_starlette_app(debug: bool = False) -> Starlette:
    """Create a Starlette application that can server the provied mcp server with SSE."""
    MESSAGES_PATH = f"{PREFIX_URL}/messages/"
    sse = SseServerTransport(MESSAGES_PATH)

    async def handle_sse(request: Request) -> Response:

        async with sse.connect_sse(
            request.scope,
            request.receive,
            request._send,
        ) as (read_stream, write_stream):
            clog.info(f"[DEBUG] SSE connection established")
            connector_id = request.path_params.get("connector_id")
            clog.info(f"Connector ID: {connector_id}")
            stdio_params = await fetch_connector_details(connector_id)
            streams = None  # Initialize streams to None for finally block safety
            try:
                async with stdio_client(stdio_params) as streams_tuple, ClientSession(
                    *streams_tuple
                ) as session:
                    streams = streams_tuple  # Assign streams once context is entered successfully
                    mcp_server = await create_proxy_server(session)

                    # Create tasks for the server logic and the disconnect monitor
                    server_task = asyncio.create_task(
                        mcp_server.run(
                            read_stream,
                            write_stream,
                            mcp_server.create_initialization_options(),
                        )
                    )
                    # try:
                    #     await mcp_server.run(
                    #         read_stream,
                    #         write_stream,
                    #         mcp_server.create_initialization_options(),
                    #     )
                    # except asyncio.CancelledError:
                    #     clog.info(
                    #         f"Server task cancelled for {connector_id} due to client disconnect"
                    #     )

                    ############## Commented by Trinanjan Saha ##############
                    # Since the SSE transport already handles disconnections, Monitor
                    # from sse.py documentation
                    # Note: The handle_sse function must return a Response to avoid a "TypeError: 'NoneType'
                    # object is not callable" error when client disconnects. The example above returns
                    # an empty Response() after the SSE connection ends to fix this.
                    # check sse.py for more details
                    ########################################################

                    monitor_task = asyncio.create_task(monitor_disconnect(request))
                    # Wait for either the server task or the monitor task to complete
                    done, pending = await asyncio.wait(
                        {server_task, monitor_task}, return_when=asyncio.FIRST_COMPLETED
                    )

                    # If the monitor finished first, it means the client disconnected
                    if monitor_task in done:
                        clog.info(f"Disconnect monitor finished for connector {connector_id}. Cancelling server task...")
                        server_task.cancel()
                        # Await the cancellation to ensure cleanup happens
                        try:
                            await server_task
                        except asyncio.CancelledError:
                            clog.info(
                                "Server task cancelled successfully due to disconnect."
                            )
                        except Exception as e_cancel:
                            # Log errors during cancellation itself if necessary
                            clog.info(f"Error during server task cancellation: {e_cancel}")
                    else:
                        # Server task finished (normally or with error), cancel the monitor
                        monitor_task.cancel()
                        try:
                            await monitor_task  # Allow monitor task to clean up
                        except asyncio.CancelledError:
                            pass  # Expected cancellation
                        # If server task finished with an error, retrieve and potentially raise it
                        if server_task in done:
                            exc = server_task.exception()
                            if exc:
                                clog.info(f"Server task finished with exception: {str(exc)}")
                                # raise exc  # Propagate the original server error

                    ############## comment ends here ##############

            except Exception as e:
                # Catches exceptions during setup, server run, cancellation, or disconnect
                clog.info(
                    f"Error in session or connection handling of connector {connector_id}: {str(e)}"
                )
                # If server_task exists and was cancelled, this might catch CancelledError
                # or the original exception if server_task failed before cancellation.
            finally:
                # This block now reliably executes after disconnection or task completion/error
                clog.info("Executing finally block for cleanup.")
                # Ensure process is terminated if it's still running
                # Check if streams was successfully assigned before trying to access it
                if streams and hasattr(streams[0], "process") and streams[0].process:
                    if streams[0].process.returncode is None:
                        clog.info(f"Terminating process for {connector_id}")
                        streams[0].process.terminate()
                        try:
                            # Use asyncio.wait_for for timeout with process wait
                            await asyncio.wait_for(
                                streams[0].process.wait(), timeout=5.0
                            )
                            clog.info(f"Process for {connector_id} terminated gracefully.")
                        except asyncio.TimeoutError:
                            clog.info(
                                f"Forcibly killing process for {connector_id} after timeout."
                            )
                            streams[0].process.kill()
                        except ProcessLookupError:
                            clog.info(f"Process for {connector_id} already terminated.")
                        except Exception as e_term:
                            clog.info(f"Error during process termination: {e_term}")
                else:
                    clog.info(
                        "No process found associated with streams or streams not initialized."
                    )
            return Response()

    return Starlette(
        debug=debug,
        routes=[
            Route(f"{PREFIX_URL}/ready", endpoint=check_readiness, methods=["GET"]),
            Route(f"{PREFIX_URL}/live", endpoint=check_readiness, methods=["GET"]),
            Route(PREFIX_URL + "/sse/{connector_id}", endpoint=handle_sse),
            Mount(MESSAGES_PATH, app=sse.handle_post_message),
        ],
    )


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="Run MCP SSE-based server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8200, help="Port to listen on")
    args = parser.parse_args()

    # Bind SSE request handling to MCP server
    starlette_app = create_starlette_app(debug=True)
    uvicorn.run(starlette_app, host=args.host, port=args.port)


# #################### below code is used to handle sse error with return Response(), monitor process is commented with this
# dockers don't get destroyed with this code


# import os

# from dotenv import load_dotenv

# load_dotenv(os.getenv("APP_CONFIG_PATH"))
# load_dotenv(os.getenv("VAULT_CONFIG_PATH"))

# APP_HOST_IP = os.getenv("APP_HOST_IP")
# PREFIX_URL = os.getenv("PREFIX_URL")

# from starlette.applications import Starlette
# from mcp.server.sse import SseServerTransport
# from starlette.requests import Request
# from starlette.responses import JSONResponse, Response
# from starlette.routing import Mount, Route
# import uvicorn
# import asyncio

# from mcp.client.session import ClientSession
# from mcp.client.stdio import StdioServerParameters, stdio_client
# from proxy_server import create_proxy_server
# from datetime import datetime

# from database.mongodb import AsyncMongoConnection


# async def check_liveness(request: Request):
#     output_json = {"output": "success"}
#     output_json.update({"message": f"queryengine service is live on {datetime.now()}"})
#     return JSONResponse(content=output_json, status_code=200)


# async def check_readiness(request: Request):
#     output_json = {"output": "success"}
#     output_json.update({"message": f"queryengine service is ready on {datetime.now()}"})
#     return JSONResponse(content=output_json, status_code=200)


# def get_secret(key, alias_keys: list, secrets: dict):

#     value = secrets.get(key)
#     if value:
#         return value

#     for alias_key in alias_keys:
#         value = secrets.get(alias_key)
#         if value:
#             return value

#     return None


# async def get_runtime_args_and_envs(
#     connection_details: list,
#     database,
#     runtime_args_info: list[dict],
#     envs_info: list[dict],
# ):
#     if not (runtime_args_info or envs_info):
#         return [], {}, []

#     tool_name = connection_details[0]
#     user_id = None
#     if len(connection_details) > 1:
#         user_id = connection_details[1]

#     mcp_cred_collection = database.get_collection("mcp_credentials")
#     filter_conditions = [{"tool_name": tool_name}]
#     if user_id:
#         filter_conditions.append({"user_id": user_id})

#     credentials = await mcp_cred_collection.find_one(filter={"$and": filter_conditions})
#     secrets = credentials.get("secrets", {})

#     runtime_args = []
#     for arg in runtime_args_info:
#         arg_name = arg.get("name")
#         arg_value = get_secret(arg_name, arg.get("alias", []), secrets)
#         if arg_value:
#             runtime_args.append(f"--{arg_name}={arg_value}")

#     envs = {}
#     env_args = []
#     for env in envs_info:
#         env_name = env.get("name")
#         env_value = get_secret(env_name, env.get("alias", []), secrets)
#         if env_value:
#             envs[env_name] = env_value
#             env_args.append("-e")
#             env_args.append(f"{env_name}={env_value}")

#     return runtime_args, envs, env_args


# async def validate_configurations(connector_split, configuration, database):

#     if configuration.get("transport") == "stdio":
#         args = configuration.get("args", [])
#         runtime_args_info = configuration.get("runtime_args", [])
#         envs_info = configuration.get("env", [])

#         runtime_args, envs, env_args = await get_runtime_args_and_envs(
#             connector_split, database, runtime_args_info, envs_info
#         )

#         for env in env_args:
#             args.insert(-1, env)

#         args.extend(runtime_args)

#         return StdioServerParameters(
#             command=configuration.get("command", ""), args=args, env=envs
#         )

#     return None


# async def fetch_connector_details(connector_id: str):

#     try:
#         database = await AsyncMongoConnection().get_connection(db_name="genai_studio")
#         connector_split = connector_id.split("-", 2)
#         collection = database.get_collection("mcp_tool_configuration")
#         tool_config = await collection.find_one(filter={"name": connector_split[0]})

#         if not tool_config:
#             raise ValueError(f"Invalid connector id: {connector_id}")

#         config = await validate_configurations(
#             connector_split, tool_config.get("configurations", {}), database=database
#         )

#         if not config:
#             raise ValueError(f"Invalid connector id: {connector_id}")
#         return config
#     except Exception as err:
#         print(str(err))


# async def monitor_disconnect(request: Request):
#     """Monitors the request connection and returns when disconnected."""
#     while True:
#         if await request.is_disconnected():
#             print("Client disconnected detected by monitor.")
#             return
#         # Check connection status periodically
#         await asyncio.sleep(1)


# def create_starlette_app(debug: bool = False) -> Starlette:
#     """Create a Starlette application that can server the provied mcp server with SSE."""
#     MESSAGES_PATH = f"{PREFIX_URL}/messages/"
#     sse = SseServerTransport(MESSAGES_PATH)

#     async def handle_sse(request: Request) -> Response:

#         async with sse.connect_sse(
#             request.scope,
#             request.receive,
#             request._send,
#         ) as (read_stream, write_stream):
#             print(f"[DEBUG] SSE connection established")
#             connector_id = request.path_params.get("connector_id")
#             print(f"Connector ID: {connector_id}")
#             stdio_params = await fetch_connector_details(connector_id)
#             streams = None  # Initialize streams to None for finally block safety
#             try:
#                 async with stdio_client(stdio_params) as streams_tuple, ClientSession(
#                     *streams_tuple
#                 ) as session:
#                     streams = streams_tuple  # Assign streams once context is entered successfully
#                     mcp_server = await create_proxy_server(session)

#                     # Create tasks for the server logic and the disconnect monitor
#                     # server_task = asyncio.create_task(
#                     #     mcp_server.run(
#                     #         read_stream,
#                     #         write_stream,
#                     #         mcp_server.create_initialization_options(),
#                     #     )
#                     # )
#                     try:
#                         await mcp_server.run(
#                             read_stream,
#                             write_stream,
#                             mcp_server.create_initialization_options(),
#                         )
#                     except asyncio.CancelledError:
#                         print(
#                             f"Server task cancelled for {connector_id} due to client disconnect"
#                         )

#                     ############## Commented by Trinanjan Saha ##############
#                     # Since the SSE transport already handles disconnections, Monitor
#                     # from sse.py documentation
#                     # Note: The handle_sse function must return a Response to avoid a "TypeError: 'NoneType'
#                     # object is not callable" error when client disconnects. The example above returns
#                     # an empty Response() after the SSE connection ends to fix this.
#                     # check sse.py for more details
#                     ########################################################

#                     # monitor_task = asyncio.create_task(monitor_disconnect(request))
#                     # # Wait for either the server task or the monitor task to complete
#                     # done, pending = await asyncio.wait(
#                     #     {server_task, monitor_task}, return_when=asyncio.FIRST_COMPLETED
#                     # )

#                     # # If the monitor finished first, it means the client disconnected
#                     # if monitor_task in done:
#                     #     print(f"Disconnect monitor finished for connector {connector_id}. Cancelling server task...")
#                     #     server_task.cancel()
#                     #     # Await the cancellation to ensure cleanup happens
#                     #     try:
#                     #         await server_task
#                     #     except asyncio.CancelledError:
#                     #         print(
#                     #             "Server task cancelled successfully due to disconnect."
#                     #         )
#                     #     except Exception as e_cancel:
#                     #         # Log errors during cancellation itself if necessary
#                     #         print(f"Error during server task cancellation: {e_cancel}")
#                     # else:
#                     #     # Server task finished (normally or with error), cancel the monitor
#                     #     monitor_task.cancel()
#                     #     try:
#                     #         await monitor_task  # Allow monitor task to clean up
#                     #     except asyncio.CancelledError:
#                     #         pass  # Expected cancellation
#                     #     # If server task finished with an error, retrieve and potentially raise it
#                     #     if server_task in done:
#                     #         exc = server_task.exception()
#                     #         if exc:
#                     #             print(f"Server task finished with exception: {str(exc)}")
#                     #             # raise exc  # Propagate the original server error

#                     ############## comment ends here ##############

#             except Exception as e:
#                 # Catches exceptions during setup, server run, cancellation, or disconnect
#                 print(
#                     f"Error in session or connection handling of connector {connector_id}: {str(e)}"
#                 )
#                 # If server_task exists and was cancelled, this might catch CancelledError
#                 # or the original exception if server_task failed before cancellation.
#             finally:
#                 # This block now reliably executes after disconnection or task completion/error
#                 print("Executing finally block for cleanup.")
#                 # Ensure process is terminated if it's still running
#                 # Check if streams was successfully assigned before trying to access it
#                 if streams and hasattr(streams[0], "process") and streams[0].process:
#                     if streams[0].process.returncode is None:
#                         print(f"Terminating process for {connector_id}")
#                         streams[0].process.terminate()
#                         try:
#                             # Use asyncio.wait_for for timeout with process wait
#                             await asyncio.wait_for(
#                                 streams[0].process.wait(), timeout=5.0
#                             )
#                             print(f"Process for {connector_id} terminated gracefully.")
#                         except asyncio.TimeoutError:
#                             print(
#                                 f"Forcibly killing process for {connector_id} after timeout."
#                             )
#                             streams[0].process.kill()
#                         except ProcessLookupError:
#                             print(f"Process for {connector_id} already terminated.")
#                         except Exception as e_term:
#                             print(f"Error during process termination: {e_term}")
#                 else:
#                     print(
#                         "No process found associated with streams or streams not initialized."
#                     )
#             return Response()

#     return Starlette(
#         debug=debug,
#         routes=[
#             Route(f"{PREFIX_URL}/ready", endpoint=check_readiness, methods=["GET"]),
#             Route(f"{PREFIX_URL}/live", endpoint=check_readiness, methods=["GET"]),
#             Route(PREFIX_URL + "/sse/{connector_id}", endpoint=handle_sse),
#             Mount(MESSAGES_PATH, app=sse.handle_post_message),
#         ],
#     )


# if __name__ == "__main__":

#     import argparse

#     parser = argparse.ArgumentParser(description="Run MCP SSE-based server")
#     parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
#     parser.add_argument("--port", type=int, default=8200, help="Port to listen on")
#     args = parser.parse_args()

#     # Bind SSE request handling to MCP server
#     starlette_app = create_starlette_app(debug=True)
#     uvicorn.run(starlette_app, host=args.host, port=args.port)
