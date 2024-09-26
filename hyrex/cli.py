import importlib
import pkgutil
import sys
from pathlib import Path

import typer

from hyrex.models import create_tables

cli = typer.Typer()


@cli.command()
def run_worker(
    task_path: str = typer.Argument(..., help="Task module path (i.e. my-app.tasks)")
):
    """
    Run the worker using the specified task package path
    """
    # Make sure the task path is in the PYTHONPATH so we can import from it
    sys.path.append(str(Path.cwd()))

    try:
        # Import the hyrex module
        hyrex_module = importlib.import_module(f"{task_path}.hyrex")

        # Convert the task path to a directory path
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
        hyrex_instance.run_worker(num_threads=8)

    except ModuleNotFoundError as e:
        typer.echo(f"Error: {e}")
        sys.exit(1)


@cli.command()
def init(conn_string: str = typer.Argument(..., help="Database connection string")):
    """
    Creates the tables for hyrex tasks/workers in the given Postgres database
    """
    create_tables(conn_string)


if __name__ == "__main__":
    cli()
