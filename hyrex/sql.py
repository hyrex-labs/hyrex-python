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
SET status = 'running', started = CURRENT_TIMESTAMP
WHERE id = next_task.id
RETURNING id, task_name, args;
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
SET status = 'running', started = CURRENT_TIMESTAMP
FROM next_task
WHERE hyrextask.id = next_task.id
RETURNING hyrextask.id, hyrextask.task_name, hyrextask.args;
"""


MARK_TASK_SUCCESS = """
    UPDATE hyrextask 
       SET status = 'success', finished = CURRENT_TIMESTAMP
       WHERE id = %s
"""

MARK_TASK_QUEUED = """
   UPDATE hyrextask 
   SET status = 'queued' 
   WHERE id = %s
"""

MARK_TASK_FAILED = """
    UPDATE hyrextask 
    SET status = 'failed' 
    WHERE id = %s
"""

SAVE_RESULTS = """
    INSERT INTO public.hyrextaskresult (task_id, results)
    VALUES (%s,  %s);
"""
