import importlib
import logging
import os
import pkgutil
import sys
from enum import Enum
from pathlib import Path

import typer

from hyrex import constants
from hyrex.app import EnvVars
from hyrex.models import create_tables

cli = typer.Typer()


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@cli.command()
def run_worker(
    app: str = typer.Argument(..., help="Module path to the Hyrex app instance"),
    queue: str = typer.Option(
        constants.DEFAULT_QUEUE,
        "--queue",
        "-q",
        help="The name of the queue to process",
    ),
    num_threads: int = typer.Option(
        8, "--num-threads", "-n", help="Number of threads to run"
    ),
    log_level: LogLevel = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Set the log level",
        case_sensitive=False,
        show_default=True,
        show_choices=True,
    ),
):
    """
    Run the worker using the specified task module path
    """
    if EnvVars.DATABASE_URL not in os.environ and EnvVars.API_KEY not in os.environ:
        typer.echo(
            f"{EnvVars.API_KEY} or {EnvVars.DATABASE_URL} must be set to run Hyrex worker."
        )
        return

    sys.path.append(str(Path.cwd()))

    try:
        module_path, instance_name = app.split(":")
        # Import the hyrex module
        hyrex_module = importlib.import_module(module_path)
        hyrex_instance = getattr(hyrex_module, instance_name)
        hyrex_instance.run_worker(
            num_threads=num_threads,
            queue=queue,
            log_level=getattr(logging, log_level.upper()),
        )

    except ModuleNotFoundError as e:
        typer.echo(f"Error: {e}")
        sys.exit(1)


@cli.command()
def init_db(
    database_string: str = typer.Option(
        os.getenv(EnvVars.DATABASE_URL),
        "--database-string",
        help="Database connection string",
    )
):
    """
    Creates the tables for hyrex tasks/workers in the given Postgres database
    """
    if database_string:
        create_tables(database_string)
        typer.echo("Hyrex tables initialized.")
        return

    typer.echo(
        f"Error: Database connection string must be provided either through the --database-string flag or the {EnvVars.DATABASE_URL} env variable."
    )


if __name__ == "__main__":
    cli()
