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
from hyrex.worker.worker_node import WorkerNode

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
        None, "--num-processes", "-p", help="Number of executor processes to run"
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
    if log_level:
        logging.basicConfig(level=getattr(logging, log_level.upper()))

    try:
        sys.path.append(str(Path.cwd()))
        module_path, instance_name = worker_module_path.split(":")
        # Import the worker module
        worker_module = importlib.import_module(module_path)
        worker_instance = getattr(worker_module, instance_name)
        if queue:
            worker_instance.set_queue(queue)
        if num_processes:
            worker_instance.set_num_processes(num_processes)
        worker_instance.run()

    except ModuleNotFoundError as e:
        typer.echo(f"Error: {e}")
        sys.exit(1)


# For internal usage only - to be removed.
@cli.command()
def worker_process(
    app_module: str = typer.Argument(..., help="Module path to the Hyrex app instance"),
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
    if log_level:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "[PID: %(process)d] %(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
        )
        logger = logging.getLogger("hyrex")
        logger.setLevel(level=getattr(logging, log_level.upper()))
        logger.addHandler(handler)

    if worker_id is None:
        worker_id = uuid7()
    elif not is_valid_uuid(worker_id):
        raise ValueError("Worker ID must be a valid UUID.")

    sys.path.append(str(Path.cwd()))

    try:
        module_path, instance_name = app_module.split(":")
        # Import the worker module
        worker_module = importlib.import_module(module_path)
        worker_instance = getattr(worker_module, instance_name)
        worker_instance.set_worker_id(worker_id)
        worker_instance.set_queue(queue)
        worker_instance.run()

    except ModuleNotFoundError as e:
        typer.echo(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
