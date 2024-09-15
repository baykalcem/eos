import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import aiohttp
from litestar import Litestar, Router
from litestar.logging import LoggingConfig
from litestar.openapi import OpenAPIConfig

from eos.web_api.public.controllers.campaign_controller import CampaignController
from eos.web_api.public.controllers.experiment_controller import ExperimentController
from eos.web_api.public.controllers.file_controller import FileController
from eos.web_api.public.controllers.lab_controller import LabController
from eos.web_api.public.controllers.task_controller import TaskController
from eos.web_api.public.exception_handling import global_exception_handler

orchestrator_host = os.getenv("EOS_ORCHESTRATOR_HOST", "localhost")
orchestrator_port = os.getenv("EOS_ORCHESTRATOR_PORT", "8070")


@asynccontextmanager
async def orchestrator_client(app: Litestar) -> AsyncGenerator[None, None]:
    client = getattr(app.state, "client", None)
    if client is None:
        client = aiohttp.ClientSession(base_url=f"http://{orchestrator_host}:{orchestrator_port}")
        app.state.orchestrator_client = client
    try:
        yield
    finally:
        await client.close()


api_router = Router(
    path="/api",
    route_handlers=[TaskController, ExperimentController, CampaignController, LabController, FileController],
    exception_handlers={Exception: global_exception_handler},
)

logging_config = LoggingConfig(configure_root_logger=False)
app = Litestar(
    route_handlers=[api_router],
    logging_config=logging_config,
    lifespan=[orchestrator_client],
    openapi_config=OpenAPIConfig(
        title="EOS REST API",
        description="Documentation for the EOS REST API",
        version="0.5.0",
        path="/docs",
    ),
)
