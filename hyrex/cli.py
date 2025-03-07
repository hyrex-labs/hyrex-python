import importlib
import logging
import os
import sys
from enum import Enum
from pathlib import Path
from uuid import UUID

import typer
from uuid_extensions import uuid7

from hyrex import constants
from hyrex.env_vars import EnvVars
from hyrex.init_db import init_postgres_db
from hyrex.worker.logging import LogLevel
from hyrex.worker.root_process import WorkerRootProcess

cli = typer.Typer()


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
        init_postgres_db(database_string)
        typer.echo("Hyrex tables initialized.")
        return

    typer.echo(
        f"Error: Database connection string must be provided either through the --database-string flag or the {EnvVars.DATABASE_URL} env variable."
    )


def validate_app_module_path(app_module_path):
    try:
        sys.path.append(str(Path.cwd()))
        module_path, instance_name = app_module_path.split(":")
        # Import the worker module
        app_module = importlib.import_module(module_path)
        app_instance = getattr(app_module, instance_name)
    except ModuleNotFoundError as e:
        typer.echo(f"Error: {e}")
        sys.exit(1)


@cli.command()
def run_worker(
    app_module_path: str = typer.Argument(..., help="Module path to the Hyrex app"),
    queue_pattern: str = typer.Option(
        constants.ANY_QUEUE,
        "--queue-pattern",
        "-q",
        help="Which queue(s) to pull tasks from. Glob patterns supported. Defaults to `*`",
    ),
    num_processes: int = typer.Option(
        8, "--num-processes", "-p", help="Number of executor processes to run"
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
    Run a Hyrex worker for the specified app module path
    """

    if not os.environ.get(EnvVars.DATABASE_URL):
        raise EnvironmentError(
            f"{EnvVars.DATABASE_URL} must be set to run Hyrex worker."
        )

    # Prevents HyrexRegistry instances from creating their own dispatchers
    os.environ[EnvVars.WORKER_PROCESS] = "true"

    validate_app_module_path(app_module_path)
    # TODO: Validate queue pattern?

    try:
        worker_root = WorkerRootProcess(
            log_level=log_level.upper(),
            app_module_path=app_module_path,
            queue_pattern=queue_pattern,
            num_processes=num_processes,
        )
        worker_root.run()

    except Exception as e:
        typer.echo(f"Error running worker: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
