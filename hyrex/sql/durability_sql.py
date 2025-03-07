QUEUE_WAITING_TASKS = """
UPDATE hyrex_task_run
SET 
    status = 'queued'::task_run_status,
    queued = NOW()
WHERE 
    status = 'waiting'::task_run_status
    AND scheduled_start < NOW();
"""
