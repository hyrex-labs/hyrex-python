import os

from hyrex.dispatcher.performance_dispatcher import PerformanceDispatcher
from hyrex.env_vars import EnvVars

from .dispatcher import Dispatcher
from .sqlc_dispatcher import SqlcDispatcher

# Single global dispatcher instance
_global_dispatcher: Dispatcher | None = None


def get_dispatcher() -> Dispatcher:
    """
    Get or create a singleton dispatcher instance.

    The first call to this function determines which dispatcher type will be used
    for the entire process. Subsequent calls return the same instance.

    The dispatcher type is determined by environment variables:
    - If API_KEY is set: Uses PerformanceDispatcher (cloud-based dispatcher)
    - If DATABASE_URL is set: Uses SqlcDispatcher (self-hosted PostgreSQL)

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
    else:
        raise ValueError(
            f"Hyrex requires either {EnvVars.DATABASE_URL} or {EnvVars.API_KEY} to be set."
        )

    return _global_dispatcher
