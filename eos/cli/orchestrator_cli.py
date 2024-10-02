import asyncio
import contextlib
import functools
import os
import signal
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import Annotated

import typer
import uvicorn
from litestar import Litestar, Router
from litestar.di import Provide
from litestar.logging import LoggingConfig
from omegaconf import OmegaConf, DictConfig

from eos.logging.logger import log, LogLevel
from eos.orchestration.orchestrator import Orchestrator
from eos.persistence.service_credentials import ServiceCredentials
from eos.web_api.orchestrator.controllers.campaign_controller import CampaignController
from eos.web_api.orchestrator.controllers.experiment_controller import ExperimentController
from eos.web_api.orchestrator.controllers.file_controller import FileController
from eos.web_api.orchestrator.controllers.lab_controller import LabController
from eos.web_api.orchestrator.controllers.task_controller import TaskController
from eos.web_api.orchestrator.exception_handling import global_exception_handler

default_config = {
    "user_dir": "./user",
    "labs": [],
    "experiments": [],
    "log_level": "INFO",
    "web_api": {
        "host": "localhost",
        "port": 8070,
    },
    "db": {
        "host": "localhost",
        "port": 27017,
        "username": None,
        "password": None,
    },
    "file_db": {
        "host": "localhost",
        "port": 9004,
        "username": None,
        "password": None,
    },
}

eos_banner = r"""The Experiment Orchestration System
 ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄
▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌
▐░█▀▀▀▀▀▀▀▀▀ ▐░█▀▀▀▀▀▀▀█░▌▐░█▀▀▀▀▀▀▀▀▀
▐░█▄▄▄▄▄▄▄▄▄ ▐░▌       ▐░▌▐░█▄▄▄▄▄▄▄▄▄
▐░░░░░░░░░░░▌▐░▌       ▐░▌▐░░░░░░░░░░░▌
▐░█▀▀▀▀▀▀▀▀▀ ▐░▌       ▐░▌ ▀▀▀▀▀▀▀▀▀█░▌
▐░█▄▄▄▄▄▄▄▄▄ ▐░█▄▄▄▄▄▄▄█░▌ ▄▄▄▄▄▄▄▄▄█░▌
▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌
 ▀▀▀▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀▀▀▀
"""


def load_config(config_file: str) -> DictConfig:
    if not Path(config_file).exists():
        raise FileNotFoundError(f"Config file '{config_file}' does not exist")
    return OmegaConf.merge(OmegaConf.create(default_config), OmegaConf.load(config_file))


def parse_list_arg(arg: str | None) -> list[str]:
    return [item.strip() for item in arg.split(",")] if arg else []


@contextlib.asynccontextmanager
async def handle_shutdown(
    orchestrator: Orchestrator, web_api_server: uvicorn.Server
) -> AbstractAsyncContextManager[None]:
    class GracefulExit(SystemExit):
        pass

    loop = asyncio.get_running_loop()
    shutdown_initiated = False

    def signal_handler(*_) -> None:
        nonlocal shutdown_initiated
        if not shutdown_initiated:
            log.warning("Shut down signal received.")
            shutdown_initiated = True
            raise GracefulExit()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, functools.partial(signal_handler))

    try:
        yield
    except GracefulExit:
        pass
    finally:
        log.info("Shutting down the internal web API server...")
        web_api_server.should_exit = True
        await web_api_server.shutdown()

        log.info("Shutting down the orchestrator...")
        orchestrator.terminate()

        log.info("EOS shut down.")


async def run_all(orchestrator: Orchestrator, web_api_server: uvicorn.Server) -> None:
    async with handle_shutdown(orchestrator, web_api_server):
        orchestrator_task = asyncio.create_task(orchestrator.spin())
        web_server_task = asyncio.create_task(web_api_server.serve())

        await asyncio.gather(orchestrator_task, web_server_task)


def start_orchestrator(
    config_file: Annotated[
        str, typer.Option("--config", "-c", help="Path to the EOS configuration file")
    ] = "./config.yml",
    user_dir: (
        Annotated[str, typer.Option("--user-dir", "-u", help="The directory containing EOS user configurations")] | None
    ) = None,
    labs: (
        Annotated[str, typer.Option("--labs", "-l", help="Comma-separated list of lab configurations to load")] | None
    ) = None,
    experiments: (
        Annotated[
            str,
            typer.Option("--experiments", "-e", help="Comma-separated list of experiment configurations to load"),
        ]
        | None
    ) = None,
    log_level: Annotated[LogLevel, typer.Option("--log-level", "-v", help="Logging level")] = None,
) -> None:

    typer.echo(eos_banner)

    file_config = load_config(config_file)
    cli_config = {}
    if user_dir is not None:
        cli_config["user_dir"] = user_dir
    if labs is not None:
        cli_config["labs"] = parse_list_arg(labs)
    if experiments is not None:
        cli_config["experiments"] = parse_list_arg(experiments)
    if log_level is not None:
        cli_config["log_level"] = log_level.value
    config = OmegaConf.merge(file_config, OmegaConf.create(cli_config))

    log.set_level(config.log_level)

    # Set up the orchestrator
    db_credentials = ServiceCredentials(**config.db)
    file_db_credentials = ServiceCredentials(**config.file_db)
    orchestrator = Orchestrator(config.user_dir, db_credentials, file_db_credentials)
    orchestrator.load_labs(config.labs)
    orchestrator.load_experiments(config.experiments)
    log.info("EOS initialized.")

    # Set up the web API server
    logging_config = LoggingConfig(
        configure_root_logger=False,
        loggers={
            "litestar": {"level": "CRITICAL"},
        },
    )
    os.environ["LITESTAR_WARN_IMPLICIT_SYNC_TO_THREAD"] = "0"

    def orchestrator_provider() -> Orchestrator:
        return orchestrator

    api_router = Router(
        path="/api",
        route_handlers=[TaskController, ExperimentController, CampaignController, LabController, FileController],
        dependencies={"orchestrator": Provide(orchestrator_provider)},
        exception_handlers={Exception: global_exception_handler},
    )
    web_api_app = Litestar(
        route_handlers=[api_router],
        logging_config=logging_config,
        exception_handlers={Exception: global_exception_handler},
    )
    config = uvicorn.Config(web_api_app, host=config.web_api.host, port=config.web_api.port, log_level="critical")
    web_api_server = uvicorn.Server(config)

    asyncio.run(run_all(orchestrator, web_api_server))
