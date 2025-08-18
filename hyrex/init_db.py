from sqlalchemy import create_engine

from hyrex.dispatcher.sqlc import (
    create_cron_job_for_sql_query_sync,
    create_cron_job_for_sql_query,
    create_enums_sync,
    create_tables_sync,
    create_functions_sync,
)
from hyrex.dispatcher.sqlc.fill_historical_task_status_counts_table import FILL_HISTORICAL_TASK_STATUS_COUNTS_TABLE
from hyrex.dispatcher.sqlc.set_orphaned_task_execution_to_lost_and_retry import SET_ORPHANED_TASK_EXECUTION_TO_LOST_AND_RETRY
from hyrex.dispatcher.sqlc.set_executor_to_lost_if_no_heartbeat import SET_EXECUTOR_TO_LOST_IF_NO_HEARTBEAT
from hyrex.dispatcher.sqlc.advance_stuck_workflows import ADVANCE_STUCK_WORKFLOWS


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

        # Register cron jobs for system tasks
        # Remove SQLC escape sequences from queries for PostgreSQL execution
        def clean_sqlc_query(query: str) -> str:
            """Remove SQLC-specific escape sequences from SQL queries."""
            # Remove the SQLC comment line with the name directive
            lines = query.strip().split('\n')
            if lines and '-- name:' in lines[0]:
                lines = lines[1:]
            cleaned = '\n'.join(lines)
            # Replace SQLC escape sequences \\: with just :
            cleaned = cleaned.replace('\\:', ':')
            return cleaned
        
        # 1. Fill historical task status counts every minute
        create_cron_job_for_sql_query_sync(
            conn,
            create_cron_job_for_sql_query.CreateCronJobForSqlQueryParams(
                jobname="FillHistoryTaskCountsTable",
                schedule="* * * * *",  # Every minute
                command=clean_sqlc_query(FILL_HISTORICAL_TASK_STATUS_COUNTS_TABLE),
                should_backfill=False,
            ),
        )

        # 2. Set orphaned running tasks to lost every minute
        create_cron_job_for_sql_query_sync(
            conn,
            create_cron_job_for_sql_query.CreateCronJobForSqlQueryParams(
                jobname="SetOrphanedRunningTaskToLost",
                schedule="* * * * *",  # Every minute
                command=clean_sqlc_query(SET_ORPHANED_TASK_EXECUTION_TO_LOST_AND_RETRY),
                should_backfill=False,
            ),
        )

        # 3. Set executors to lost if no heartbeat every minute
        create_cron_job_for_sql_query_sync(
            conn,
            create_cron_job_for_sql_query.CreateCronJobForSqlQueryParams(
                jobname="SetExecutorToLostIfNoHeartbeat",
                schedule="* * * * *",  # Every minute
                command=clean_sqlc_query(SET_EXECUTOR_TO_LOST_IF_NO_HEARTBEAT),
                should_backfill=False,
            ),
        )

        # 4. Advance stuck workflows every 2 minutes
        create_cron_job_for_sql_query_sync(
            conn,
            create_cron_job_for_sql_query.CreateCronJobForSqlQueryParams(
                jobname="AdvanceStuckWorkflows",
                schedule="*/2 * * * *",  # Every 2 minutes
                command=clean_sqlc_query(ADVANCE_STUCK_WORKFLOWS),
                should_backfill=False,
            ),
        )
