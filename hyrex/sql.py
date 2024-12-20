CREATE_HYREX_TASK_EXECUTION_TABLE = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type
        WHERE typname = 'statusenum'
            AND typnamespace = 'public'::regnamespace
    ) THEN
        CREATE TYPE statusenum AS ENUM (
            'success',
            'failed',
            'running',
            'queued',
            'up_for_cancel',
            'canceled',
            'lost',
            'waiting'
        );
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS hyrex_task_execution (
    id              UUID                        NOT NULL PRIMARY KEY,
    durable_id      UUID                        NOT NULL,
    root_id         UUID                        NOT NULL,
    parent_id       UUID,
    task_name       VARCHAR                     NOT NULL,
    args            JSON                        NOT NULL,
    queue           VARCHAR                     NOT NULL,
    max_retries     SMALLINT                    NOT NULL,
    priority        SMALLINT                    NOT NULL,
    status          statusenum                  NOT NULL DEFAULT 'queued'::statusenum,
    attempt_number  SMALLINT                    NOT NULL DEFAULT 0,
    scheduled_start TIMESTAMP WITH TIME ZONE,
    executor_id     UUID,
    queued          TIMESTAMP WITH TIME ZONE             DEFAULT CURRENT_TIMESTAMP,
    started         TIMESTAMP WITH TIME ZONE,
    last_heartbeat  TIMESTAMP WITH TIME ZONE,
    finished        TIMESTAMP WITH TIME ZONE,
    idempotency_key VARCHAR
);

CREATE INDEX IF NOT EXISTS ix_hyrex_task_execution_task_name
    ON hyrex_task_execution (task_name);

CREATE INDEX IF NOT EXISTS ix_hyrex_task_execution_status
    ON hyrex_task_execution (status);

CREATE INDEX IF NOT EXISTS ix_hyrex_task_execution_queue
    ON hyrex_task_execution (queue);

CREATE INDEX IF NOT EXISTS ix_hyrex_task_execution_scheduled_start
    ON hyrex_task_execution (scheduled_start);

CREATE INDEX IF NOT EXISTS index_queue_status
    ON hyrex_task_execution (status, queue, scheduled_start, task_name);

CREATE UNIQUE INDEX IF NOT EXISTS ix_hyrex_task_execution_idempotency_key
    ON public.hyrex_task_execution (task_name, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
"""

CREATE_HYREX_TASK_TABLE = """
CREATE TABLE IF NOT EXISTS hyrex_task (
    task_name    TEXT NOT NULL PRIMARY KEY,
    cron_expr    TEXT,
    source_code  TEXT,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
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
CREATE TABLE IF NOT EXISTS hyrex_task_result (
    id SERIAL PRIMARY KEY,
    task_id UUID REFERENCES hyrex_task_execution(id),
    result JSON DEFAULT '{}'
);
"""

CREATE_HYREX_EXECUTOR_TABLE = """
CREATE TABLE IF NOT EXISTS hyrex_executor (
    id             UUID                        NOT NULL PRIMARY KEY,
    name           VARCHAR                     NOT NULL,
    queue          VARCHAR                     NOT NULL,
    started        TIMESTAMP WITH TIME ZONE             DEFAULT CURRENT_TIMESTAMP,
    last_heartbeat TIMESTAMP WITH TIME ZONE,
    stopped        TIMESTAMP WITH TIME ZONE
);
"""

FETCH_TASK = """
WITH next_task AS (
    SELECT id 
    FROM hyrex_task_execution
    WHERE
        queue = $1 AND
        status = 'queued'
    ORDER BY priority DESC, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE hyrex_task_execution as ht
SET status = 'running', started = CURRENT_TIMESTAMP, last_heartbeat = CURRENT_TIMESTAMP, executor_id = $2
FROM next_task
WHERE ht.id = next_task.id
RETURNING ht.id, ht.durable_id, ht.root_id, ht.parent_id, ht.task_name, ht.args, ht.queue, ht.priority, ht.scheduled_start, ht.queued, ht.started;
"""

FETCH_TASK_WITH_CONCURRENCY = """
WITH lock_result AS (
    SELECT pg_try_advisory_xact_lock(hashtext($1)) AS lock_acquired
),
next_task AS (
    SELECT id
    FROM hyrex_task_execution, lock_result
    WHERE
        lock_acquired = TRUE
        AND queue = $1
        AND status = 'queued'
        AND (SELECT COUNT(*) FROM hyrex_task_execution WHERE queue = $1 AND status = 'running') < $2
    ORDER BY priority DESC, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE hyrex_task_execution as ht
SET status = 'running', started = CURRENT_TIMESTAMP, last_heartbeat = CURRENT_TIMESTAMP, executor_id = $3
FROM next_task
WHERE ht.id = next_task.id
RETURNING ht.id, ht.durable_id, ht.root_id, ht.parent_id, ht.task_name, ht.args, ht.queue, ht.priority, ht.scheduled_start, ht.queued, ht.started;
"""

CONDITIONALLY_RETRY_TASK = """
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
        idempotency_key
    FROM hyrex_task_execution
    WHERE id = $1
      AND attempt_number < max_retries
)
INSERT INTO hyrex_task_execution (
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
    idempotency_key
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
    idempotency_key
FROM existing_task;
"""

UPSERT_TASK = """
INSERT INTO hyrex_task (task_name, cron_expr, source_code, last_updated)
VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
ON CONFLICT (task_name)
DO UPDATE SET 
    cron_expr = EXCLUDED.cron_expr,
    source_code = EXCLUDED.source_code,
    last_updated = CURRENT_TIMESTAMP;
"""

ENQUEUE_TASK = """
WITH task_insertion AS (
        INSERT INTO hyrex_task_execution (
                                          id,
                                          durable_id,
                                          root_id,
                                          parent_id,
                                          task_name,
                                          args,
                                          queue,
                                          max_retries,
                                          priority,
                                          idempotency_key
            )
            VALUES (
                       $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
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
                             'idempotency_key', $10,
                             'task_name', $5,
                             'queue', $7
                     )
                 WHERE NOT EXISTS (SELECT 1 FROM task_insertion)
                   AND $10 IS NOT NULL
         )
    SELECT EXISTS (SELECT 1 FROM task_insertion) as task_created;
"""

MARK_TASK_SUCCESS = """
    UPDATE hyrex_task_execution 
    SET status = 'success', finished = CURRENT_TIMESTAMP
    WHERE id = $1 AND status = 'running'
"""

MARK_TASK_FAILED = """
    UPDATE hyrex_task_execution 
    SET status = 'failed', finished = CURRENT_TIMESTAMP
    WHERE id = $1 AND status = 'running'
"""

TRY_TO_CANCEL_TASK = """
    UPDATE hyrex_task_execution
    SET status = CASE 
                WHEN status = 'running' THEN 'up_for_cancel'::statusenum 
                WHEN status = 'queued' THEN 'canceled'::statusenum
                END
    WHERE id = $1 AND status IN ('running', 'queued');
"""

TASK_CANCELED = """
    UPDATE hyrex_task_execution
    SET status = 'canceled'
    WHERE id = $1 AND status = 'up_for_cancel';
"""

GET_TASKS_UP_FOR_CANCEL = """
    SELECT id FROM hyrex_task_execution WHERE status = 'up_for_cancel'
"""

GET_TASK_STATUS = """
    SELECT status FROM hyrex_task_execution WHERE id = $1
"""

TASK_HEARTBEAT = """
    UPDATE hyrex_task_execution 
    SET last_heartbeat = $1 
    WHERE id = ANY($2)
"""

EXECUTOR_HEARTBEAT = """
    UPDATE hyrex_executor 
    SET last_heartbeat = $1 
    WHERE id = ANY($2)
"""

REGISTER_EXECUTOR = """
    INSERT INTO hyrex_executor (id, name, queue, started, last_heartbeat)
    VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
"""

DISCONNECT_EXECUTOR = """
    UPDATE hyrex_executor
    SET stopped = CURRENT_TIMESTAMP
    WHERE id = $1 AND stopped IS NULL
"""

MARK_RUNNING_TASKS_LOST = """
    UPDATE hyrex_task_execution
    SET status = 'lost'
    WHERE status = 'running' AND executor_id = $1
"""

SAVE_RESULT = """
    INSERT INTO hyrex_task_result (task_id, result)
    VALUES ($1, $2);
"""

GET_UNIQUE_QUEUES_FOR_PATTERN = """
    SELECT DISTINCT queue FROM hyrex_task_execution WHERE status = 'queued' AND queue ~ $1
"""

MARK_LOST_TASKS = """
    SELECT id, task_name, queue, last_heartbeat
    FROM hyrex_task_execution
    WHERE status = 'running'::statusenum
    AND last_heartbeat < NOW() - INTERVAL '5 minutes';
"""

MARK_LOST_EXECUTORS = """TODO"""
