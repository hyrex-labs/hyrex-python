import psycopg

from hyrex.sql import cron_sql, sql, stats_sql, workflow_sql


def create_tables(conn_string):
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
