CREATE_HYREX_TASK_RUN_TABLE = """
-- Create task_run_status enum type if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 
                   FROM pg_type 
                   WHERE typname = 'task_run_status' 
                     AND typnamespace = 'public'::regnamespace) THEN
        CREATE TYPE public.task_run_status AS ENUM (
            'success',
            'failed',
            'running',
            'queued',
            'up_for_cancel',
            'canceled',
            'waiting',
            'lost',
            'skipped'
        );
    END IF;
END $$;

-- Create task run table (renamed from hyrex_task_execution)
CREATE TABLE IF NOT EXISTS hyrex_task_run (
    id              UUID                        NOT NULL PRIMARY KEY,
    durable_id      UUID                        NOT NULL,
    root_id         UUID                        NOT NULL,
    parent_id       UUID,
    workflow_run_id UUID DEFAULT NULL,
    workflow_dependencies UUID[] DEFAULT NULL,
    task_name       VARCHAR                     NOT NULL,
    args            JSON                        NOT NULL,
    queue           VARCHAR                     NOT NULL,
    max_retries     SMALLINT                    NOT NULL,
    priority        SMALLINT                    NOT NULL,
    timeout_seconds INT                         DEFAULT NULL CHECK (timeout_seconds IS NULL OR timeout_seconds > 0),
    status          task_run_status             NOT NULL,
    attempt_number  SMALLINT                    NOT NULL,
    scheduled_start TIMESTAMP WITH TIME ZONE,
    executor_id     UUID,
    queued          TIMESTAMP WITH TIME ZONE,
    started         TIMESTAMP WITH TIME ZONE,
    finished        TIMESTAMP WITH TIME ZONE,
    last_heartbeat  TIMESTAMP WITH TIME ZONE,
    idempotency_key VARCHAR,
    log_link        VARCHAR
);

-- Partial index for quickly fetching queued tasks by queue, priority, and queued timestamp.
CREATE INDEX IF NOT EXISTS idx_hyrex_task_run_queued
    ON hyrex_task_run (queue, priority ASC, queued ASC)
    WHERE (status = 'queued');

-- Partial index for quickly counting tasks that are running in a particular queue.
CREATE INDEX IF NOT EXISTS idx_hyrex_task_run_running
    ON hyrex_task_run (queue)
    WHERE (status = 'running');

-- Unique index for task_name + idempotency_key
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_task_idempotency 
ON hyrex_task_run (task_name, idempotency_key) 
WHERE idempotency_key IS NOT NULL;
"""

CREATE_HYREX_TASK_TABLE = """
CREATE TABLE IF NOT EXISTS hyrex_task (
    task_name      TEXT NOT NULL PRIMARY KEY,
    cron_expr      TEXT,
    source_code    TEXT,
    default_config JSON,
    arg_schema     JSON,
    last_updated   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_SYSTEM_LOG_TABLE = """
CREATE TABLE IF NOT EXISTS hyrex_system_logs (
    id UUID NOT NULL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE,
    event_name VARCHAR NOT NULL,
    event_body JSON NOT NULL
);
"""

CREATE_HYREX_RESULT_TABLE = """
CREATE TABLE IF NOT EXISTS hyrex_task_result
(
    task_id    UUID PRIMARY KEY REFERENCES public.hyrex_task_run (id) ON DELETE CASCADE,
    result     JSON,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_HYREX_APP_TABLE = """
    CREATE TABLE IF NOT EXISTS hyrex_app (
          id    BIGSERIAL NOT NULL PRIMARY KEY,
          app_info JSON
    );
"""

REGISTER_APP_INFO_SQL = """
    INSERT INTO hyrex_app (
        id,
        app_info
    ) VALUES (
        $1,
        $2
    )
    ON CONFLICT (id) DO UPDATE SET
        app_info = $2;
"""

CREATE_HYREX_EXECUTOR_TABLE = """
DO $$
    BEGIN
        -- Create enum if it doesn't exist
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'executor_status') THEN
            CREATE TYPE executor_status AS ENUM ('SHUTDOWN', 'LOST', 'RUNNING', 'UNKNOWN');
        END IF;
END$$;

-- Create or replace the table with the status column
CREATE TABLE IF NOT EXISTS hyrex_executor
(
    id             UUID      NOT NULL PRIMARY KEY,
    name           VARCHAR   NOT NULL,
    worker_name    VARCHAR   NOT NULL,
    queue_pattern  VARCHAR   NOT NULL,
    queues         VARCHAR[] NOT NULL,
    started        TIMESTAMP WITH TIME ZONE,
    stopped        TIMESTAMP WITH TIME ZONE,
    last_heartbeat TIMESTAMP WITH TIME ZONE,
    stats          JSON,
    status         executor_status NOT NULL DEFAULT 'UNKNOWN'
);
"""

FETCH_TASK = """
WITH next_task AS (
    SELECT id 
    FROM hyrex_task_run
    WHERE
        queue = $1
        AND status = 'queued'
        AND task_name = ANY($3)
    ORDER BY priority ASC, queued
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE hyrex_task_run as ht
SET status = 'running', started = CURRENT_TIMESTAMP, last_heartbeat = CURRENT_TIMESTAMP, executor_id = $2
FROM next_task
WHERE ht.id = next_task.id
RETURNING ht.id, ht.durable_id, ht.root_id, ht.parent_id, ht.task_name, ht.args, ht.queue, ht.priority, ht.timeout_seconds, ht.scheduled_start, ht.queued, ht.started, ht.workflow_run_id, ht.attempt_number, ht.max_retries;
"""


# TODO: Consider this alternative:
# FETCH_TASK_WITH_CONCURRENCY = """
# WITH current_running AS (
#     SELECT COUNT(*) AS count FROM hyrex_task_run WHERE queue = $1 AND status = 'running'
# ),
# next_task AS (
#     SELECT id
#     FROM hyrex_task_run, current_running
#     WHERE
#         queue = $1
#         AND status = 'queued'
#         AND task_name = ANY($4)
#         AND current_running.count < $2
#     ORDER BY priority ASC, queued
#     FOR UPDATE SKIP LOCKED
#     LIMIT 1
# )
# UPDATE hyrex_task_run as ht
# SET status = 'running', started = CURRENT_TIMESTAMP, last_heartbeat = CURRENT_TIMESTAMP, executor_id = $3
# FROM next_task
# WHERE ht.id = next_task.id
# RETURNING ht.id, ht.durable_id, ht.root_id, ht.parent_id, ht.task_name, ht.args, ht.queue,
#           ht.priority, ht.timeout_seconds, ht.scheduled_start, ht.queued, ht.started,
#           ht.workflow_run_id, ht.attempt_number, ht.max_retries;
# """
FETCH_TASK_WITH_CONCURRENCY = """
WITH lock_result AS (
    SELECT pg_try_advisory_xact_lock(hashtext($1)) AS lock_acquired
),
next_task AS (
    SELECT id
    FROM hyrex_task_run, lock_result
    WHERE
        lock_acquired = TRUE
        AND queue = $1
        AND status = 'queued'
        AND task_name = ANY($4)
        AND (SELECT COUNT(*) FROM hyrex_task_run WHERE queue = $1 AND status = 'running') < $2
    ORDER BY priority ASC, queued
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE hyrex_task_run as ht
SET status = 'running', started = CURRENT_TIMESTAMP, last_heartbeat = CURRENT_TIMESTAMP, executor_id = $3
FROM next_task
WHERE ht.id = next_task.id
RETURNING ht.id, ht.durable_id, ht.root_id, ht.parent_id, ht.task_name, ht.args, ht.queue, ht.priority, ht.timeout_seconds, ht.scheduled_start, ht.queued, ht.started, ht.workflow_run_id, ht.attempt_number, ht.max_retries;
"""

CREATE_RETRY_TASK = """
WITH existing_task AS (
    SELECT
        durable_id,
        root_id,
        parent_id,
        task_name,
        args,
        queue,
        attempt_number,
        max_retries,
        priority,
        timeout_seconds,
        idempotency_key,
        workflow_run_id,
        workflow_dependencies
    FROM hyrex_task_run
    WHERE id = $1
)
INSERT INTO hyrex_task_run (
    id,
    durable_id,
    root_id,
    parent_id,
    queued,
    status,
    task_name,
    args,
    queue,
    attempt_number,
    max_retries,
    priority,
    timeout_seconds,
    idempotency_key,
    workflow_run_id,
    workflow_dependencies
)
SELECT
    $2 AS id,
    durable_id,
    root_id,
    parent_id,
    CURRENT_TIMESTAMP as queued,
    'queued' AS status,
    task_name,
    args,
    queue,
    attempt_number + 1 AS attempt_number,
    max_retries,
    priority,
    timeout_seconds,
    idempotency_key,
    workflow_run_id,
    workflow_dependencies
FROM existing_task;
"""

CREATE_RETRY_TASK_WITH_BACKOFF = """
WITH existing_task AS (
    SELECT
        durable_id,
        root_id,
        parent_id,
        task_name,
        args,
        queue,
        attempt_number,
        max_retries,
        priority,
        timeout_seconds,
        idempotency_key,
        workflow_run_id,
        workflow_dependencies
    FROM hyrex_task_run
    WHERE id = $1
)
INSERT INTO hyrex_task_run (
    id,
    durable_id,
    root_id,
    parent_id,
    scheduled_start,
    status,
    task_name,
    args,
    queue,
    attempt_number,
    max_retries,
    priority,
    timeout_seconds,
    idempotency_key,
    workflow_run_id,
    workflow_dependencies
)
SELECT
    $2 AS id,
    durable_id,
    root_id,
    parent_id,
    $3 AS scheduled_start,
    'waiting' AS status,
    task_name,
    args,
    queue,
    attempt_number + 1 AS attempt_number,
    max_retries,
    priority,
    timeout_seconds,
    idempotency_key,
    workflow_run_id,
    workflow_dependencies
FROM existing_task;
"""

UPSERT_TASK = """
INSERT INTO hyrex_task (task_name, arg_schema, default_config, cron_expr, source_code, last_updated)
VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
ON CONFLICT (task_name)
DO UPDATE SET 
    arg_schema = EXCLUDED.arg_schema,
    default_config = EXCLUDED.default_config,
    cron_expr = EXCLUDED.cron_expr,
    source_code = EXCLUDED.source_code,
    last_updated = CURRENT_TIMESTAMP;
"""

ENQUEUE_TASK = """
WITH task_insertion AS (
        INSERT INTO hyrex_task_run (
                                          id,
                                          durable_id,
                                          root_id,
                                          parent_id,
                                          task_name,
                                          args,
                                          queue,
                                          max_retries,
                                          priority,
                                          timeout_seconds,
                                          idempotency_key,
                                          status,
                                          queued,
                                          attempt_number,
                                          workflow_run_id,
                                          workflow_dependencies
            )
            VALUES (
                       $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, CURRENT_TIMESTAMP, 0, $13, $14
                   )
            ON CONFLICT (task_name, idempotency_key)
                WHERE idempotency_key IS NOT NULL
                DO NOTHING
            RETURNING id
    ),
         log_entry AS (
             INSERT INTO hyrex_system_logs (
                                            id,
                                            timestamp,
                                            event_name,
                                            event_body
                 )
                 SELECT
                     gen_random_uuid(),
                     CURRENT_TIMESTAMP,
                     'IDEMPOTENCY_COLLISION',
                     json_build_object(
                             'attempted_task_id', $1,
                             'idempotency_key', $11,
                             'task_name', $5,
                             'queue', $7
                     )
                 WHERE NOT EXISTS (SELECT 1 FROM task_insertion)
                   AND $10 IS NOT NULL
         )
    SELECT EXISTS (SELECT 1 FROM task_insertion) as task_created;
"""

MARK_TASK_SUCCESS = """
    UPDATE hyrex_task_run 
    SET status = 'success', finished = CURRENT_TIMESTAMP
    WHERE id = $1 AND status = 'running'
"""

MARK_TASK_FAILED = """
    UPDATE hyrex_task_run 
    SET status = 'failed', finished = CURRENT_TIMESTAMP
    WHERE id = $1 AND status = 'running'
"""

TRY_TO_CANCEL_TASK = """
    UPDATE hyrex_task_run
    SET status = CASE 
                WHEN status = 'running' THEN 'up_for_cancel'::task_run_status
                WHEN status = 'queued' THEN 'canceled'::task_run_status
                WHEN status = 'waiting' THEN 'canceled'::task_run_status
                END
    WHERE id = $1 AND status IN ('running', 'queued', 'waiting');
"""

TRY_TO_CANCEL_DURABLE_RUN = """
    UPDATE hyrex_task_run
    SET status = CASE 
                WHEN status = 'running' THEN 'up_for_cancel'::task_run_status
                WHEN status = 'queued' THEN 'canceled'::task_run_status
                END
    WHERE durable_id = $1 AND status IN ('running', 'queued');
"""

TASK_CANCELED = """
    UPDATE hyrex_task_run
    SET status = 'canceled'
    WHERE id = $1 AND status = 'up_for_cancel';
"""

GET_TASKS_UP_FOR_CANCEL = """
    SELECT id FROM hyrex_task_run WHERE status = 'up_for_cancel'
"""

GET_TASK_STATUS = """
    SELECT status FROM hyrex_task_run WHERE id = $1
"""

SET_LOG_LINK = """
    UPDATE hyrex_task_run
    SET log_link = $2
    WHERE id = $1
"""

TASK_HEARTBEAT = """
    UPDATE hyrex_task_run 
    SET last_heartbeat = $1 
    WHERE id = ANY($2)
"""

EXECUTOR_HEARTBEAT = """
    UPDATE hyrex_executor 
    SET last_heartbeat = $1 
    WHERE id = ANY($2)
"""

UPDATE_EXECUTOR_STATS = """
    UPDATE hyrex_executor
    SET last_heartbeat = CURRENT_TIMESTAMP,
        stats          = $2
    WHERE id = $1;
"""

REGISTER_EXECUTOR = """
    INSERT INTO hyrex_executor (id,
                                name,
                                queue_pattern,
                                queues,
                                worker_name,
                                started,
                                stopped,
                                last_heartbeat)
    VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, null, CURRENT_TIMESTAMP);
"""

DISCONNECT_EXECUTOR = """
    UPDATE hyrex_executor
    SET stopped = CURRENT_TIMESTAMP
    WHERE id = $1 AND stopped IS NULL
"""

MARK_RUNNING_TASKS_LOST = """
    UPDATE hyrex_task_run
    SET status = 'lost'
    WHERE status = 'running' AND executor_id = $1
"""

SAVE_RESULT = """
    INSERT INTO hyrex_task_result (task_id, result)
    VALUES ($1, $2);
"""


FETCH_RESULT = """
SELECT result FROM hyrex_task_result WHERE task_id = $1;
"""

GET_UNIQUE_QUEUES_FOR_PATTERN = """
    SELECT DISTINCT queue FROM hyrex_task_run WHERE status = 'queued' AND queue ~ $1
"""

GET_QUEUES_FOR_PATTERN = """
    WITH distinct_queues AS (SELECT DISTINCT queue
                             FROM hyrex_task_run
                             WHERE status = 'queued'
                               AND queue ~ $1),
         queue_count AS (SELECT COUNT(*) AS cnt
                         FROM distinct_queues)
    SELECT queue
    FROM (
             -- If count <= 100000, just select all queues
             SELECT dq.queue
             FROM distinct_queues dq,
                  queue_count qc
             WHERE qc.cnt <= 100000

             UNION ALL

             -- If count > 100000, select a random subset
             SELECT queue
             FROM (SELECT dq.queue,
                          row_number() OVER (ORDER BY random()) AS rn
                   FROM distinct_queues dq,
                        queue_count qc
                   WHERE qc.cnt > 100000) sub
             WHERE rn <= 100000) final_result;"""

GET_TASK_RUNS_BY_DURABLE_ID = """
    SELECT 
        tr.id,
        tr.task_name,
        tr.max_retries,
        tr.attempt_number,
        tr.status,
        tr.queued,
        tr.started,
        tr.finished,
        tres.result
    FROM 
        hyrex_task_run tr
    LEFT JOIN 
        hyrex_task_result tres ON tr.id = tres.task_id
    WHERE 
        tr.durable_id = $1
    ORDER BY 
        tr.queued DESC
"""

MARK_LOST_TASKS = """
    SELECT id, task_name, queue, last_heartbeat
    FROM hyrex_task_run
    WHERE status = 'running'::task_run_status
    AND last_heartbeat < NOW() - INTERVAL '5 minutes';
"""

MARK_LOST_EXECUTORS = """TODO"""

UPDATE_EXECUTOR_QUEUES = """
    UPDATE hyrex_executor
    SET 
        queues = $2,
        last_heartbeat = CURRENT_TIMESTAMP
    WHERE id = $1;
"""

GET_WORKFLOW_DURABLE_RUNS = """
    SELECT DISTINCT durable_id
    FROM hyrex_task_run
    WHERE workflow_run_id = $1
    ORDER BY durable_id;
"""
