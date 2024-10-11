INIT_DB = """
-- Enable the uuid-ossp extension for UUID generation functions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create the status_enum type based on your StatusEnum
CREATE TYPE status_enum AS ENUM ('queued', 'running', 'completed', 'failed');

-- Create the hyrex_task table
CREATE TABLE hyrex_task (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v7(),
    root_id UUID NOT NULL,

    -- Indexed fields
    task_name TEXT NOT NULL,
    status status_enum NOT NULL DEFAULT 'queued',
    queue TEXT NOT NULL DEFAULT 'default',
    scheduled_start TIMESTAMP WITH TIME ZONE,

    worker_id UUID,

    queued TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started TIMESTAMP WITH TIME ZONE,
    finished TIMESTAMP WITH TIME ZONE,

    max_retries INTEGER NOT NULL DEFAULT 0,
    attempt_number INTEGER NOT NULL DEFAULT 0,

    args JSONB NOT NULL DEFAULT '{}'::jsonb,

    priority INTEGER NOT NULL CHECK (priority BETWEEN 1 AND 10)
);

-- Create indexes on the specified fields
CREATE INDEX idx_hyrex_task_task_name ON hyrex_task(task_name);
CREATE INDEX idx_hyrex_task_status ON hyrex_task(status);
CREATE INDEX idx_hyrex_task_queue ON hyrex_task(queue);
CREATE INDEX idx_hyrex_task_scheduled_start ON hyrex_task(scheduled_start);
"""


FETCH_TASK = """
WITH next_task AS (
    SELECT id 
    FROM hyrextask
    WHERE
        queue = %s AND
        status = 'queued'
    ORDER BY id
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
    ORDER BY id
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
        max_retries
    FROM hyrextask
    WHERE id = %(existing_id)s
      AND attempt_number < max_retries
)
INSERT INTO hyrextask (
    id,
    root_id,
    status,
    task_name,
    args,
    queue,
    attempt_number,
    max_retries
)
SELECT
    %(new_id)s AS id,
    root_id,
    'queued' AS status,
    task_name,
    args,
    queue,
    attempt_number + 1 AS attempt_number,
    max_retries
FROM existing_task;
"""

GET_TASK_BY_ID = """
SELECT root_id, task_name, args, queue, attempt_number, max_retries FROM hyrextask WHERE id = %s
"""

INSERT_TASK = """
INSERT INTO hyrextask (id, root_id, status, task_name, args, queue, attempt_number, max_retries) VALUES (%(id)s, %(root_id)s, 'queued', %(task_name)s, %(args)s, %(queue)s, %(attempt_number)s, %(max_retries)s)
"""


MARK_TASK_SUCCESS = """
    UPDATE hyrextask 
       SET status = 'success', finished = CURRENT_TIMESTAMP
       WHERE id = %s
"""

MARK_TASK_QUEUED = """
   UPDATE hyrextask 
   SET status = 'queued', worker_id = NULL
   WHERE id = %s
"""

MARK_TASK_FAILED = """
    UPDATE hyrextask 
    SET status = 'failed', finished = CURRENT_TIMESTAMP
    WHERE id = %s
"""

SAVE_RESULTS = """
    INSERT INTO public.hyrextaskresult (task_id, results)
    VALUES (%s,  %s);
"""
