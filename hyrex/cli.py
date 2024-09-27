import importlib
import logging
import os
import pkgutil
import sys
from pathlib import Path
from enum import Enum

import typer

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
    task_path: str = typer.Argument(..., help="Task module path (i.e. my-app.tasks)"),
    queue: str = typer.Option(
        "default", "--queue", "-q", help="The name of the queue to process"
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
    if EnvVars.DATABASE_URL not in os.environ:
        typer.echo(f"{EnvVars.DATABASE_URL} must be set to run Hyrex worker.")
        return

    sys.path.append(str(Path.cwd()))

    try:
        # Import the hyrex module
        hyrex_module = importlib.import_module(f"{task_path}.hyrex")

        # Convert the task module path to a directory path
        spec = importlib.util.find_spec(task_path)
        if spec is None or spec.submodule_search_locations is None:
            raise ImportError(f"Could not find module path for '{task_path}'")
        task_directory = spec.submodule_search_locations[0]

        # Import and register all task modules
        for _, module_name, _ in pkgutil.iter_modules([task_directory]):
            if module_name != "hyrex":  # Skip the hyrex config itself
                importlib.import_module(f"{task_path}.{module_name}")

        # Now that all tasks are registered, start the worker
        hyrex_instance = hyrex_module.hy
        hyrex_instance.run_worker(
            num_threads=num_threads, log_level=getattr(logging, log_level.upper())
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
