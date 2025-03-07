import psycopg

from hyrex.sql import cron_sql, durability_sql, sql, stats_sql, workflow_sql


def register_cron_sql_query(
    conn: psycopg.Connection,
    cron_job_name: str,
    cron_sql_query: str,
    cron_expr: str,
    should_backfill: bool,
) -> None:
    """Register a new cron job for executing a SQL query on a schedule."""
    with psycopg.RawCursor(conn) as cur:
        cur.execute(
            cron_sql.CREATE_CRON_JOB_FOR_SQL_QUERY,
            [cron_expr, cron_sql_query, cron_job_name, should_backfill],
        )


def init_postgres_db(conn_string):
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(sql.CREATE_HYREX_APP_TABLE)
            cur.execute(sql.CREATE_HYREX_TASK_RUN_TABLE)
            cur.execute(sql.CREATE_HYREX_TASK_TABLE)
            cur.execute(sql.CREATE_SYSTEM_LOG_TABLE)
            cur.execute(sql.CREATE_HYREX_RESULT_TABLE)
            cur.execute(sql.CREATE_HYREX_EXECUTOR_TABLE)
            cur.execute(cron_sql.CREATE_HYREX_CRON_JOB_TABLE)
            cur.execute(cron_sql.CREATE_HYREX_CRON_JOB_RUN_DETAILS_TABLE)
            cur.execute(cron_sql.CREATE_HYREX_SCHEDULER_LOCK_TABLE)
            cur.execute(cron_sql.CREATE_EXECUTE_QUEUED_COMMAND_FUNCTION)
            cur.execute(stats_sql.CREATE_HISTORICAL_TASK_STATUS_COUNTS)
            cur.execute(workflow_sql.CREATE_WORKFLOW_TABLE)
            cur.execute(workflow_sql.CREATE_WORKFLOW_RUN_TABLE)
        conn.commit()

        register_cron_sql_query(
            conn,
            cron_job_name="QueueWaitingTasks",
            cron_expr="* * * * *",
            cron_sql_query=durability_sql.QUEUE_WAITING_TASKS,
            should_backfill=False,
        )

        register_cron_sql_query(
            conn,
            cron_job_name="FillHistoryTaskCountsTable",
            cron_expr="* * * * *",
            cron_sql_query=stats_sql.FILL_HISTORICAL_TASK_STATUS_COUNTS_TABLE,
            should_backfill=False,
        )

        register_cron_sql_query(
            conn,
            cron_job_name="SetOrphanedRunningTaskToLost",
            cron_expr="* * * * *",
            cron_sql_query=durability_sql.SET_ORPHANED_TASK_EXECUTION_TO_LOST_AND_RETRY,
            should_backfill=False,
        )

        register_cron_sql_query(
            conn,
            cron_job_name="SetExecutorToLostIfNoHeartbeat",
            cron_expr="* * * * *",
            cron_sql_query=durability_sql.SET_EXECUTOR_TO_LOST_IF_NO_HEARTBEAT,
            should_backfill=False,
        )

        # await this.registerCronSQLQuery({
        #     cronJobName: "SetOrphanedRunningTaskToLost",
        #     cronExpr: "* * * * *",
        #     cronSqlQuery: durabilitySQL.SET_ORPHANED_TASK_EXECUTION_TO_LOST_AND_RETRY,
        #     shouldBackfill: false
        # })

        # await this.registerCronSQLQuery({
        #     cronJobName: "SetExecutorToLostIfNoHeartbeat",
        #     cronExpr: "* * * * *",
        #     cronSqlQuery: durabilitySQL.SET_EXECUTOR_TO_LOST_IF_NO_HEARTBEAT,
        #     shouldBackfill: false
        # })
