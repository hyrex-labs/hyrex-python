QUEUE_WAITING_TASKS = """
UPDATE hyrex_task_run
SET 
    status = 'queued'::task_run_status,
    queued = NOW()
WHERE 
    status = 'waiting'::task_run_status
    AND scheduled_start < NOW();
"""

SET_ORPHANED_TASK_EXECUTION_TO_LOST_AND_RETRY = """
    WITH lost_tasks AS (
        UPDATE hyrex_task_run
            SET status = 'lost'
            WHERE status = 'running'
                AND (
                      executor_id IS NULL
                          OR NOT EXISTS (
                          SELECT 1
                          FROM hyrex_executor
                          WHERE hyrex_executor.id = hyrex_task_run.executor_id
                            AND hyrex_executor.status = 'RUNNING'
                      )
                      )
            RETURNING *
    )
    INSERT INTO hyrex_task_run (
        id,
        durable_id,
        root_id,
        parent_id,
        workflow_run_id,
        workflow_dependencies,
        task_name,
        args,
        queue,
        max_retries,
        priority,
        timeout_seconds,
        status,
        attempt_number,
        idempotency_key,
        queued
    )
    SELECT
        gen_random_uuid(),
        durable_id,
        root_id,
        parent_id,
        workflow_run_id,
        workflow_dependencies,
        task_name,
        args,
        queue,
        max_retries,
        priority,
        timeout_seconds,
        'queued',
        attempt_number + 1,
        idempotency_key,
        NOW()
    FROM lost_tasks
    WHERE attempt_number < max_retries;
"""

SET_EXECUTOR_TO_LOST_IF_NO_HEARTBEAT = """
    WITH lost_executors AS (
        UPDATE hyrex_executor
            SET status = 'LOST'
            WHERE status = 'RUNNING'
                AND (
                      last_heartbeat IS NULL
                          OR last_heartbeat < (NOW() - INTERVAL '5 minutes')
                      )
            RETURNING id, last_heartbeat
    )
    INSERT INTO hyrex_system_logs (
        id,
        timestamp,
        event_name,
        event_body
    )
    SELECT
        gen_random_uuid(),
        NOW(),
        'EXECUTOR_LOST',
        json_build_object(
                'executor_id', id,
                'last_heartbeat', last_heartbeat,
                'reason', CASE
                              WHEN last_heartbeat IS NULL THEN 'No heartbeat recorded'
                              ELSE 'Heartbeat timeout exceeded 5 minutes'
                    END
        )
    FROM lost_executors;
"""
