import os
import subprocess
import sys
from typing import Annotated

import typer


# ruff: noqa: S603


def start_web_api(
    host: Annotated[str, typer.Option("--host", help="Host for the EOS web API server")] = "localhost",
    port: Annotated[int, typer.Option("--port", help="Port for the EOS web API server")] = 8000,
    orchestrator_host: Annotated[
        str, typer.Option("--orchestrator-host", help="Host for the EOS orchestrator server")
    ] = "localhost",
    orchestrator_port: Annotated[
        int, typer.Option("--orchestrator-port", help="Port for the EOS orchestrator server")
    ] = 8070,
) -> None:
    env = os.environ.copy()
    env["EOS_ORCHESTRATOR_HOST"] = str(orchestrator_host)
    env["EOS_ORCHESTRATOR_PORT"] = str(orchestrator_port)

    subprocess.run(
        [sys.executable, "-m", "uvicorn", "--host", str(host), "--port", str(port), "eos.web_api.public.server:app"],
        env=env,
        check=True,
    )
