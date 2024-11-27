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
from hyrex.config import EnvVars
from hyrex.models import create_tables
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
        create_tables(database_string)
        typer.echo("Hyrex tables initialized.")
        return

    typer.echo(
        f"Error: Database connection string must be provided either through the --database-string flag or the {EnvVars.DATABASE_URL} env variable."
    )


def validate_worker_module_path(worker_module_path):
    try:
        sys.path.append(str(Path.cwd()))
        module_path, instance_name = worker_module_path.split(":")
        # Import the worker module
        worker_module = importlib.import_module(module_path)
        worker_instance = getattr(worker_module, instance_name)
    except ModuleNotFoundError as e:
        typer.echo(f"Error: {e}")
        sys.exit(1)


@cli.command()
def run_worker(
    worker_module_path: str = typer.Argument(
        ..., help="Module path to the Hyrex worker"
    ),
    queue: str = typer.Option(
        None,
        "--queue",
        "-q",
        help="The name of the queue to process",
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
    Run multiple worker processes using the specified worker module path
    """

    # Prevents HyrexRegistry instances from creating their own dispatchers
    os.environ[EnvVars.WORKER_PROCESS] = "true"

    validate_worker_module_path(worker_module_path)

    try:
        worker_root = WorkerRootProcess(
            log_level=log_level.upper(),
            worker_module_path=worker_module_path,
            queue=queue,
            num_processes=num_processes,
        )
        worker_root.run()

    except Exception as e:
        typer.echo(f"Error running worker: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
