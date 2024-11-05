from hyrex import constants

CREATE_HYREX_TASK_TABLE = """
DO $$
BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'statusenum' AND typnamespace = 'public'::regnamespace) THEN
CREATE TYPE statusenum AS ENUM ('success', 'failed', 'running', 'queued', "up_for_cancel", "canceled");
END IF;
END $$;

create table if not exists hyrextask
(
    id              uuid       not null
primary key,
    root_id         uuid       not null,
    task_name       varchar    not null,
    args            json       not null,
    queue           varchar    not null,
    max_retries     smallint   not null,
    priority        smallint   not null,
    status          statusenum not null default 'queued'::statusenum,
    attempt_number  smallint   not null default 0,
    scheduled_start timestamp with time zone,
    worker_id       uuid,
    queued          timestamp with time zone default CURRENT_TIMESTAMP,
    started         timestamp with time zone,
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

CREATE_HYREX_WORKER_TABLE = """
create table if not exists hyrexworker
(
    id      uuid    not null
primary key,
    name    varchar not null,
    queue   varchar not null,
    started timestamp,
    stopped timestamp
);
"""

FETCH_TASK = """
WITH next_task AS (
    SELECT id 
    FROM hyrextask
    WHERE
        queue = %s AND
        status = 'queued'
    ORDER BY priority DESC, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE hyrextask
SET status = 'running', started = CURRENT_TIMESTAMP, worker_id = %s
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
SET status = 'running', started = CURRENT_TIMESTAMP, worker_id = %s
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
    WHERE id = %(existing_id)s
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
    %(new_id)s AS id,
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

# TODO: Update hyrextask table to have better defaults so they're not needed here.
ENQUEUE_TASK = """
INSERT INTO hyrextask (
    id,
    root_id,
    task_name,
    args,
    queue,
    max_retries,
    priority
) VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

MARK_TASK_SUCCESS = """
    UPDATE hyrextask 
    SET status = 'success', finished = CURRENT_TIMESTAMP
    WHERE id = %s
"""

MARK_TASK_FAILED = """
    UPDATE hyrextask 
    SET status = 'failed', finished = CURRENT_TIMESTAMP
    WHERE id = %s
"""

RESET_OR_CANCEL_TASK = """
   UPDATE hyrextask 
   SET status = CASE 
                   WHEN status = 'up_for_cancel' THEN 'canceled'::statusenum 
                   ELSE 'queued'::statusenum 
               END, 
       worker_id = CASE 
                     WHEN status = 'up_for_cancel' THEN worker_id  -- Keep the current value
                     ELSE NULL
                   END,
       started = CASE 
                   WHEN status = 'up_for_cancel' THEN started  -- Keep the current value
                   ELSE NULL
                 END
   WHERE id = %s
"""

MARK_TASK_CANCELED = """
    UPDATE hyrextask
    SET status = CASE 
                WHEN status = 'running' THEN 'up_for_cancel'::statusenum 
                WHEN status = 'queued' THEN 'canceled'::statusenum
                END
    WHERE id = %s AND status IN ('running', 'queued');
"""

GET_WORKERS_TO_CANCEL = """
    SELECT worker_id FROM hyrextask WHERE status = 'up_for_cancel' AND worker_id = ANY(%s);
"""

GET_TASK_STATUS = """
    SELECT status FROM hyrextask WHERE id = %s
"""

REGISTER_WORKER = """
    INSERT INTO hyrexworker (id, name, queue, started)
    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
"""

MARK_WORKER_STOPPED = """
    UPDATE hyrexworker
    SET stopped = CURRENT_TIMESTAMP
    WHERE id = %s
"""

SAVE_RESULT = """
    INSERT INTO hyrextaskresult (task_id, result)
    VALUES (%s,  %s);
"""
