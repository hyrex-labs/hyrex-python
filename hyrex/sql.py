from hyrex import constants

# CREATE_TABLES = f"""
# -- Enable UUID extension
# CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

# -- Create custom ENUM type for StatusEnum
# CREATE TYPE statusenum AS ENUM ('success', 'failed', 'canceled', 'running', 'queued');

# CREATE TABLE hyrexworker (
#     id UUID PRIMARY KEY,
#     name TEXT NOT NULL,
#     queue TEXT NOT NULL DEFAULT {constants.DEFAULT_QUEUE},
#     started TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
#     stopped TIMESTAMP WITH TIME ZONE
# );

# """

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
ENQUEUE_TASKS = """
INSERT INTO hyrextask (
    id,
    root_id,
    task_name,
    args,
    queue,
    max_retries,
    priority,
    status,
    attempt_number,
    queued
) VALUES (%s, %s, %s, %s, %s, %s, %s, 'queued', 0, CURRENT_TIMESTAMP);
"""

RESET_TASK = """
   UPDATE hyrextask 
   SET status = 'queued', worker_id = NULL, started = NULL
   WHERE id = %s
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

MARK_TASK_CANCELED = """
    UPDATE hyrextask
    SET status = 'canceled'
    WHERE id = %s AND status = 'queued'
"""

GET_TASK_STATUS = """
    SELECT status FROM hyrextask WHERE id = %s
"""
