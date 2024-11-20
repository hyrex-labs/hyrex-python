import os
from enum import StrEnum

from hyrex.config import EnvVars

from .dispatcher import Dispatcher
from .platform_dispatcher import PlatformDispatcher
from .postgres_dispatcher import PostgresDispatcher
from .postgres_lite_dispatcher import PostgresLiteDispatcher


class DispatcherType(StrEnum):
    POSTGRES = "postgres"
    POSTGRES_LITE = "postgres_lite"
    PLATFORM = "platform"


def get_dispatcher(worker: bool = False) -> Dispatcher:
    api_key = os.environ.get(EnvVars.API_KEY)
    conn_string = os.environ.get(EnvVars.DATABASE_URL)
    if api_key:
        return PlatformDispatcher(api_key=api_key)
    elif conn_string:
        if worker:
            # Single-threaded dispatcher simplifies worker
            return PostgresLiteDispatcher(conn_string=conn_string)
        return PostgresDispatcher(conn_string=conn_string)
    else:
        raise ValueError(
            f"Hyrex requires either {EnvVars.DATABASE_URL} or {EnvVars.API_KEY} to be set."
        )
