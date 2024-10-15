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
