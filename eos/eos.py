#!/usr/bin/env python3

import typer

from eos.cli.orchestrator_cli import start_orchestrator
from eos.cli.pkg_cli import pkg_app
from eos.cli.web_api_cli import start_web_api

eos_app = typer.Typer(pretty_exceptions_show_locals=False)
eos_app.command(name="orchestrator", help="Start the EOS orchestrator")(start_orchestrator)
eos_app.command(name="api", help="Start the EOS web API")(start_web_api)
eos_app.add_typer(pkg_app, name="pkg", help="Manage EOS packages")

if __name__ == "__main__":
    eos_app()
