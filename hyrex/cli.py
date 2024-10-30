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
from hyrex.app import EnvVars
from hyrex.models import create_tables
from hyrex.worker_manager import WorkerManager

cli = typer.Typer()


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def is_valid_uuid(id: UUID):
    try:
        UUID(id)
        return True
    except ValueError:
        return False


@cli.command()
def run_worker(
    app: str = typer.Argument(..., help="Module path to the Hyrex app instance"),
    queue: str = typer.Option(
        constants.DEFAULT_QUEUE,
        "--queue",
        "-q",
        help="The name of the queue to process",
    ),
    num_processes: int = typer.Option(
        8, "--num-processes", "-p", help="Number of worker processes to run"
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
    Run worker processes using the specified task module path
    """
    manager = WorkerManager(
        app=app,
        queue=queue,
        num_workers=num_processes,
        log_level=getattr(logging, log_level.upper()),
    )
    manager.run()


# For internal usage only
@cli.command()
def worker_process(
    app: str = typer.Argument(..., help="Module path to the Hyrex app instance"),
    queue: str = typer.Option(
        constants.DEFAULT_QUEUE,
        "--queue",
        "-q",
        help="The name of the queue to process",
    ),
    worker_id: str = typer.Option(
        None, "--worker-id", help="Optional UUID for the worker."
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
    Run a single worker process
    """
    if worker_id is None:
        worker_id = uuid7()
    elif not is_valid_uuid(worker_id):
        raise ValueError("Worker ID must be a valid UUID.")

    sys.path.append(str(Path.cwd()))

    try:
        module_path, instance_name = app.split(":")
        # Import the hyrex module
        hyrex_module = importlib.import_module(module_path)
        hyrex_instance = getattr(hyrex_module, instance_name)
        hyrex_instance.run_worker(
            queue=queue,
            worker_id=worker_id,
            log_level=getattr(logging, log_level.upper()),
        )

    except ModuleNotFoundError as e:
        typer.echo(f"Error: {e}")
        sys.exit(1)


# @cli.command()
# def run_worker(
#     app: str = typer.Argument(..., help="Module path to the Hyrex app instance"),
#     queue: str = typer.Option(
#         constants.DEFAULT_QUEUE,
#         "--queue",
#         "-q",
#         help="The name of the queue to process",
#     ),
#     num_threads: int = typer.Option(
#         8, "--num-threads", "-n", help="Number of threads to run"
#     ),
#     log_level: LogLevel = typer.Option(
#         "INFO",
#         "--log-level",
#         "-l",
#         help="Set the log level",
#         case_sensitive=False,
#         show_default=True,
#         show_choices=True,
#     ),
# ):
#     """
#     Run the worker using the specified task module path
#     """
#     if EnvVars.DATABASE_URL not in os.environ and EnvVars.API_KEY not in os.environ:
#         typer.echo(
#             f"{EnvVars.API_KEY} or {EnvVars.DATABASE_URL} must be set to run Hyrex worker."
#         )
#         return

#     sys.path.append(str(Path.cwd()))

#     try:
#         module_path, instance_name = app.split(":")
#         # Import the hyrex module
#         hyrex_module = importlib.import_module(module_path)
#         hyrex_instance = getattr(hyrex_module, instance_name)
#         hyrex_instance.run_worker(
#             num_threads=num_threads,
#             queue=queue,
#             log_level=getattr(logging, log_level.upper()),
#         )

#     except ModuleNotFoundError as e:
#         typer.echo(f"Error: {e}")
#         sys.exit(1)


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
