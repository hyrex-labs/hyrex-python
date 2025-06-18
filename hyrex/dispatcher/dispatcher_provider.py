import os
from enum import StrEnum

from hyrex.dispatcher.performance_dispatcher import PerformanceDispatcher
from hyrex.env_vars import EnvVars

from .dispatcher import Dispatcher
from .postgres_dispatcher import PostgresDispatcher
from .postgres_lite_dispatcher import PostgresLiteDispatcher
from .sqlc_dispatcher import SqlcDispatcher

# TODO: Clean up logic and decide if PostgresLiteDispatcher should be sunsetted.

# Single global dispatcher instance
_global_dispatcher: Dispatcher | None = None


def get_dispatcher(worker: bool = False) -> Dispatcher:
    """
    Get or create a singleton dispatcher instance.

    The first call to this function determines which dispatcher type will be used
    for the entire process. Subsequent calls return the same instance regardless
    of the parameters passed.

    Args:
        worker: Only used for the first call if a PostgreSQL dispatcher is created.
               If True, returns a PostgresLiteDispatcher which is better suited for worker processes.

    Returns:
        A Dispatcher instance, reusing the global instance if one exists.

    Raises:
        ValueError: If neither API_KEY nor DATABASE_URL environment variables are set.
    """
    global _global_dispatcher

    # If we already have a global dispatcher, return it
    if _global_dispatcher is not None:
        return _global_dispatcher

    # Create the appropriate dispatcher based on environment variables
    api_key = os.environ.get(EnvVars.API_KEY)
    conn_string = os.environ.get(EnvVars.DATABASE_URL)

    if api_key:
        _global_dispatcher = PerformanceDispatcher(
            api_key=api_key, conn_string=conn_string
        )
    elif conn_string:
        _global_dispatcher = SqlcDispatcher(conn_string=conn_string)
        # if worker:
        #     # Single-threaded dispatcher simplifies worker
        #     _global_dispatcher = PostgresLiteDispatcher(conn_string=conn_string)
        # else:
        #     _global_dispatcher = PostgresDispatcher(conn_string=conn_string)
    else:
        raise ValueError(
            f"Hyrex requires either {EnvVars.DATABASE_URL} or {EnvVars.API_KEY} to be set."
        )

    return _global_dispatcher
