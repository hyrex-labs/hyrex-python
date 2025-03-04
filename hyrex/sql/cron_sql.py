import json
from typing import Any, Dict, List

from hyrex.schemas import CronJobRun, EnqueueTaskRequest

CREATE_HYREX_CRON_JOB_TABLE = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_source_type') THEN
        CREATE TYPE job_source_type AS ENUM ('SYSTEM', 'TASK', 'APPLICATION');
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS hyrex_cron_job
(
    jobid                          bigserial PRIMARY KEY,
    schedule                       text,
    command                        text    NOT NULL,
    active                         boolean NOT NULL DEFAULT true,
    jobname                        text    NOT NULL,
    job_source                     job_source_type NOT NULL,
    activated_at                   timestamptz      default now(),
    scheduled_jobs_confirmed_until timestamptz      default now(),
    should_backfill                boolean default true,
    UNIQUE (jobname)
);
"""

# Specifically modeled on cron.job_run_details table in pg_cron
CREATE_HYREX_CRON_JOB_RUN_DETAILS_TABLE = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'cron_job_status' AND typnamespace = 'public'::regnamespace) THEN
        CREATE TYPE public.cron_job_status AS ENUM (
            'success',
            'queued',
            'failed'
        );
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS hyrex_cron_job_run_details (
  jobid        bigint      NOT NULL,
  runid        bigserial   PRIMARY KEY,
  command      text        NOT NULL,
  status       cron_job_status,
  schedule_time timestamptz not null,
  start_time   timestamptz,
  end_time     timestamptz,
  UNIQUE (jobid, schedule_time)
)
"""

CREATE_HYREX_SCHEDULER_LOCK_TABLE = """
    CREATE TABLE IF NOT EXISTS hyrex_scheduler_lock
    (
        lockid       bigserial PRIMARY KEY,
        worker_name  text        NOT NULL,
        acquired_at  timestamptz NOT NULL DEFAULT now(),
        heartbeat_at timestamptz NOT NULL DEFAULT now(),
        release_at   timestamptz NOT NULL,
        is_active    boolean     NOT NULL DEFAULT true
    );
"""

ACQUIRE_SCHEDULER_LOCK = """
INSERT INTO hyrex_scheduler_lock (
    lockid, worker_name, acquired_at, heartbeat_at, release_at, is_active
)
VALUES (
    1,                  -- single global lock id
    $1,                 -- workerName
    now(),              -- acquired_at
    now(),              -- heartbeat_at
    now() + CAST($2 AS interval),  -- release_at (e.g. now + '5 minutes')
    true                -- is_active
)
ON CONFLICT (lockid)
  DO UPDATE 
     SET worker_name  = EXCLUDED.worker_name,
         acquired_at  = EXCLUDED.acquired_at,
         heartbeat_at = EXCLUDED.heartbeat_at,
         release_at   = EXCLUDED.release_at,
         is_active    = EXCLUDED.is_active
   WHERE (
       -- Only update if the lock is not truly active,
       -- i.e. is already inactive or expired:
       hyrex_scheduler_lock.is_active = false
       OR hyrex_scheduler_lock.release_at <= now()
   )
RETURNING lockid;
"""

RELEASE_SCHEDULER_LOCK = """
    UPDATE hyrex_scheduler_lock
    SET is_active    = false,
        release_at   = now(),
        heartbeat_at = now()
    WHERE lockid = 1
      AND worker_name = $1
    RETURNING lockid;
"""

PULL_ACTIVE_CRON_EXPRESSIONS = """
    SELECT jobid,
           schedule,
           command,
           active,
           jobname,
           activated_at,
           scheduled_jobs_confirmed_until,
           should_backfill
    FROM hyrex_cron_job
    WHERE active = true
--       AND activated_at > NOW();
"""

UPDATE_CRON_JOB_CONFIRMATION_TS = """
    UPDATE hyrex_cron_job
    SET scheduled_jobs_confirmed_until = now()
    WHERE jobid = $1;
"""


def cron_job_runs_to_sql(runs: List[CronJobRun]) -> Dict[str, Any]:
    """
    Convert a list of cron job runs to SQL insert statement with dollar-sign parameterized values.

    Args:
        runs: List of CronJobRun objects with jobid, command, and schedule_time attributes

    Returns:
        dict: Contains 'sql' string and 'values' list for parameterized query
    """
    # Calculate the placeholder indices for each run
    placeholder_index = 1
    placeholder_strings = []

    # Collect all values in order
    values = []
    for run in runs:
        values.extend([run.jobid, run.command, "queued", run.schedule_time.isoformat()])

    for _ in runs:
        # Use $1, $2, etc. format instead of %(1)s
        placeholder_strings.append(
            f"(${placeholder_index}, ${placeholder_index + 1}, ${placeholder_index + 2}, ${placeholder_index + 3})"
        )
        placeholder_index += 4

    # Build this part separately to avoid Python3.11 f-string backslash limitations
    placeholder_section = ",\n            ".join(placeholder_strings)

    # Construct the final SQL string
    sql = f"""
        INSERT INTO hyrex_cron_job_run_details
            (jobid, command, status, schedule_time)
        VALUES
            {placeholder_section}
        ON CONFLICT (jobid, schedule_time) DO NOTHING
        RETURNING runid;
    """

    return {"sql": sql, "values": values}


CREATE_EXECUTE_QUEUED_COMMAND_FUNCTION = """
CREATE OR REPLACE FUNCTION execute_queued_command()
RETURNS text AS
$$
DECLARE
    cmd text;
    selected_runid bigint;
    start_ts timestamp;
BEGIN
    -- 1) Grab the first queued command (row) and lock it
    SELECT command, runid 
      INTO cmd, selected_runid
      FROM hyrex_cron_job_run_details
     WHERE status = 'queued'
       AND schedule_time <= now()
     ORDER BY schedule_time
     LIMIT 1
     FOR UPDATE SKIP LOCKED;

    IF FOUND THEN
        -- 2) Mark start_time right before execution
        start_ts := clock_timestamp();

        -- 3) Execute the command text
        EXECUTE cmd;

        -- 4) Mark the end_time and set status
        UPDATE hyrex_cron_job_run_details
           SET status      = 'success',
               start_time  = start_ts,
               end_time    = clock_timestamp()
         WHERE runid = selected_runid
           AND status = 'queued';
        
        RETURN 'executed';
    END IF;

    RETURN 'not_found';

EXCEPTION
    WHEN OTHERS THEN
        -- In the event of an error, mark job as failed and record times
        UPDATE hyrex_cron_job_run_details
           SET status      = 'failed',
               start_time  = COALESCE(start_ts, clock_timestamp()),
               end_time    = clock_timestamp()
         WHERE runid = selected_runid
           AND status = 'queued';
        RAISE;
END;
$$
LANGUAGE plpgsql;
"""


def create_insert_task_cron_expression(enqueue_task_request: EnqueueTaskRequest) -> str:
    tr = enqueue_task_request

    # Handle idempotency key exactly like the TypeScript version
    idempotency_key_str = (
        "NULL" if tr.idempotency_key is None else f"'{tr.idempotency_key}'"
    )

    # Convert args to JSON string
    args_json = json.dumps(tr.args)

    # Handle timeout_seconds which could be None
    timeout_seconds_str = (
        "NULL" if tr.timeout_seconds is None else str(tr.timeout_seconds)
    )

    sql = f"""WITH vars AS (
       SELECT gen_random_uuid() as shared_uuid
   ),
   task_insertion AS (
   INSERT INTO hyrex_task_run (
                                     id,
                                     durable_id,
                                     root_id,
                                     task_name,
                                     args,
                                     queue,
                                     max_retries,
                                     priority,
                                     timeout_seconds,
                                     status,
                                     attempt_number,
                                     queued,
                                     idempotency_key
       )
       SELECT
               v.shared_uuid,
               v.shared_uuid,
               v.shared_uuid,
               '{tr.task_name}',
               '{args_json}'::json,
               '{tr.queue}',
               {tr.max_retries},
               {tr.priority},
               {timeout_seconds_str},
               'queued'::task_run_status,
               0,
               CURRENT_TIMESTAMP,
               {idempotency_key_str}
       FROM vars v
       ON CONFLICT (task_name, idempotency_key)
           WHERE idempotency_key IS NOT NULL
           DO NOTHING
       RETURNING id),
    log_entry AS (
        INSERT INTO hyrex_system_logs (
                                       id,
                                       timestamp,
                                       event_name,
                                       event_body
            )
            SELECT gen_random_uuid(),
                   CURRENT_TIMESTAMP,
                   'IDEMPOTENCY_COLLISION',
                   json_build_object(
                           'attempted_task_id', v.shared_uuid,
                           'idempotency_key', {idempotency_key_str},
                           'task_name', '{tr.task_name}',
                           'queue', '{tr.queue}'
                   )
            FROM vars v
            WHERE NOT EXISTS (SELECT 1 FROM task_insertion)
              AND {idempotency_key_str} IS NOT NULL)
SELECT (SELECT id FROM task_insertion) as task_created;"""

    return sql


CREATE_CRON_JOB_FOR_TASK = """
    INSERT INTO hyrex_cron_job (schedule, command, jobname, job_source)
    VALUES ($1, $2, $3, 'TASK')
    ON CONFLICT (jobname) 
    DO UPDATE SET 
        activated_at = CURRENT_TIMESTAMP,
        schedule = EXCLUDED.schedule,
        command = EXCLUDED.command,
        job_source = 'TASK',
        active = true;
"""

CREATE_CRON_JOB_FOR_SQL_QUERY = """
    INSERT INTO hyrex_cron_job (schedule, command, jobname, should_backfill, job_source)
    VALUES ($1, $2, $3, $4, 'SYSTEM')
    ON CONFLICT (jobname) 
    DO UPDATE SET 
        activated_at = CURRENT_TIMESTAMP,
        schedule = EXCLUDED.schedule,
        command = EXCLUDED.command,
        should_backfill = EXCLUDED.should_backfill,
        job_source = 'SYSTEM',
        active = true;
"""

TURN_OFF_CRON_FOR_TASK = """
    UPDATE hyrex_cron_job
    SET active   = false,
        schedule = NULL
    WHERE jobname = $1
      AND job_source = 'TASK';
"""
