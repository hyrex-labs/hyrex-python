from sqlalchemy import create_engine

from hyrex.dispatcher.sqlc import (
    create_cron_job_for_sql_query_sync,
    create_cron_job_for_sql_query,
    create_enums_sync,
    create_tables_sync,
    create_functions_sync,
)


def init_postgres_db(conn_string):
    """Initialize the Postgres database with all required Hyrex tables and functions."""
    # Convert connection string to use psycopg3
    if conn_string.startswith("postgresql://"):
        conn_string = conn_string.replace("postgresql://", "postgresql+psycopg://", 1)
    elif conn_string.startswith("postgres://"):
        conn_string = conn_string.replace("postgres://", "postgresql+psycopg://", 1)

    # Create SQLAlchemy engine
    engine = create_engine(conn_string)

    with engine.begin() as conn:
        # Create enums (will skip if they already exist)
        try:
            create_enums_sync(conn)
        except Exception:
            pass  # Enums might already exist

        # Create tables and indexes
        create_tables_sync(conn)

        # Create functions and triggers
        create_functions_sync(conn)

        # TODO: Register cron jobs for system tasks
