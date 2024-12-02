from hyrex import constants

CREATE_HYREX_TASK_TABLE = """
DO $$
BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'statusenum' AND typnamespace = 'public'::regnamespace) THEN
CREATE TYPE statusenum AS ENUM ('success', 'failed', 'running', 'queued', 'up_for_cancel', 'canceled', 'lost', 'waiting');
END IF;
END $$;

create table if not exists hyrextask
(
    id              uuid       not null
primary key,
    root_id         uuid       not null,
    parent_id       uuid,
    task_name       varchar    not null,
    args            json       not null,
    queue           varchar    not null,
    max_retries     smallint   not null,
    priority        smallint   not null,
    status          statusenum not null default 'queued'::statusenum,
    attempt_number  smallint   not null default 0,
    scheduled_start timestamp with time zone,
    executor_id     uuid,
    queued          timestamp with time zone default CURRENT_TIMESTAMP,
    started         timestamp with time zone,
    last_heartbeat  timestamp with time zone,
    finished        timestamp with time zone
);

create index if not exists ix_hyrextask_task_name
on hyrextask (task_name);

create index if not exists ix_hyrextask_status
on hyrextask (status);

create index if not exists ix_hyrextask_queue
on hyrextask (queue);

create index if not exists ix_hyrextask_scheduled_start
on hyrextask (scheduled_start);

create index if not exists index_queue_status
on hyrextask (status, queue, scheduled_start, task_name);
"""

CREATE_HYREX_RESULT_TABLE = """
CREATE TABLE IF NOT EXISTS hyrextaskresult (
    id SERIAL PRIMARY KEY,
    task_id UUID REFERENCES hyrextask(id),
    result JSON DEFAULT '{}'
);
"""

CREATE_HYREX_EXECUTOR_TABLE = """
create table if not exists hyrexexecutor
(
    id      uuid    not null
primary key,
    name    varchar not null,
    queue   varchar not null,
    started timestamp with time zone default CURRENT_TIMESTAMP,
    last_heartbeat timestamp with time zone,
    stopped timestamp with time zone
);
"""

FETCH_TASK = """
WITH next_task AS (
    SELECT id 
    FROM hyrextask
    WHERE
        queue = $1 AND
        status = 'queued'
    ORDER BY priority DESC, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE hyrextask
SET status = 'running', started = CURRENT_TIMESTAMP, last_heartbeat = CURRENT_TIMESTAMP, worker_id = $2
FROM next_task
WHERE hyrextask.id = next_task.id
RETURNING hyrextask.id, hyrextask.task_name, hyrextask.args;
"""

FETCH_TASK_FROM_ANY_QUEUE = """
WITH next_task AS (
    SELECT id
    FROM hyrextask
    WHERE status = 'queued'
    ORDER BY priority DESC, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE hyrextask
SET status = 'running', started = CURRENT_TIMESTAMP, last_heartbeat = CURRENT_TIMESTAMP, executor_id = $1
FROM next_task
WHERE hyrextask.id = next_task.id
RETURNING hyrextask.id, hyrextask.task_name, hyrextask.args;
"""

CONDITIONALLY_RETRY_TASK = """
WITH existing_task AS (
    SELECT
        root_id,
        task_name,
        args,
        queue,
        attempt_number,
        max_retries,
        priority
    FROM hyrextask
    WHERE id = $1
      AND attempt_number < max_retries
)
INSERT INTO hyrextask (
    id,
    root_id,
    queued,
    status,
    task_name,
    args,
    queue,
    attempt_number,
    max_retries,
    priority
)
SELECT
    $2 AS id,
    root_id,
    CURRENT_TIMESTAMP as queued,
    'queued' AS status,
    task_name,
    args,
    queue,
    attempt_number + 1 AS attempt_number,
    max_retries,
    priority
FROM existing_task;
"""

ENQUEUE_TASK = """
INSERT INTO hyrextask (
    id,
    root_id,
    parent_id,
    task_name,
    args,
    queue,
    max_retries,
    priority
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
"""

MARK_TASK_SUCCESS = """
    UPDATE hyrextask 
    SET status = 'success', finished = CURRENT_TIMESTAMP
    WHERE id = $1 AND status = 'running'
"""

MARK_TASK_FAILED = """
    UPDATE hyrextask 
    SET status = 'failed', finished = CURRENT_TIMESTAMP
    WHERE id = $1 AND status = 'failed'
"""

RESET_OR_CANCEL_TASK = """
   UPDATE hyrextask 
   SET status = CASE 
                   WHEN status = 'up_for_cancel' THEN 'canceled'::statusenum 
                   ELSE 'queued'::statusenum 
               END, 
       executor_id = CASE 
                     WHEN status = 'up_for_cancel' THEN executor_id  -- Keep the current value
                     ELSE NULL
                   END,
       started = CASE 
                   WHEN status = 'up_for_cancel' THEN started  -- Keep the current value
                   ELSE NULL
                 END
   WHERE id = $1
"""

TRY_TO_CANCEL_TASK = """
    UPDATE hyrextask
    SET status = CASE 
                WHEN status = 'running' THEN 'up_for_cancel'::statusenum 
                WHEN status = 'queued' THEN 'canceled'::statusenum
                END
    WHERE id = $1 AND status IN ('running', 'queued');
"""

TASK_CANCELED = """
    UPDATE hyrextask
    SET status = 'canceled'
    WHERE id = $1 AND status = 'up_for_cancel';
"""

GET_TASKS_UP_FOR_CANCEL = """
    SELECT id FROM hyrextask WHERE status = 'up_for_cancel'
"""

GET_TASK_STATUS = """
    SELECT status FROM hyrextask WHERE id = $1
"""

TASK_HEARTBEAT = """
    UPDATE hyrextask 
    SET last_heartbeat = $1 
    WHERE id = ANY($2)
"""

EXECUTOR_HEARTBEAT = """
    UPDATE hyrexexecutor 
    SET last_heartbeat = $1 
    WHERE id = ANY($2)
"""

REGISTER_EXECUTOR = """
    INSERT INTO hyrexexecutor (id, name, queue, started, last_heartbeat)
    VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
"""

DISCONNECT_EXECUTOR = """
    UPDATE hyrexexecutor
    SET stopped = CURRENT_TIMESTAMP
    WHERE id = $1 AND stopped IS NULL
"""

MARK_RUNNING_TASKS_LOST = """
    UPDATE hyrextask
    SET status = 'lost'
    WHERE status = 'running' AND executor_id = $1
"""

SAVE_RESULT = """
    INSERT INTO hyrextaskresult (task_id, result)
    VALUES ($1, $2);
"""
