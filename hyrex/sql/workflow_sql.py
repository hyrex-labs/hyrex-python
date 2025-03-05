CREATE_WORKFLOW_TABLE = """
    CREATE TABLE IF NOT EXISTS hyrex_workflow
    (
        workflow_name  TEXT NOT NULL PRIMARY KEY,
        cron_expr      TEXT,
        source_code    TEXT,
        default_config JSON,
        arg_schema     JSON,
        dag_structure  JSON,
        last_updated   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
"""

UPSERT_WORKFLOW = """
INSERT INTO hyrex_workflow (workflow_name, cron_expr, source_code, dag_structure, arg_schema, default_config, last_updated)
VALUES ($1, $2, $3, $4, $5, $6, NOW())
ON CONFLICT (workflow_name)
DO UPDATE SET 
    cron_expr = EXCLUDED.cron_expr,
    source_code = EXCLUDED.source_code,
    dag_structure = EXCLUDED.dag_structure,
    arg_schema = EXCLUDED.arg_schema,
    default_config = EXCLUDED.default_config,
    last_updated = NOW();
"""

CREATE_WORKFLOW_RUN_TABLE = """
    DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 
                   FROM pg_type 
                   WHERE typname = 'workflow_run_status' 
                     AND typnamespace = 'public'::regnamespace) THEN
        CREATE TYPE public.workflow_run_status AS ENUM (
            'success',
            'failed',
            'running',
            'up_for_cancel',
            'canceled',
            'asleep'
        );
    END IF;
END $$;

-- Create workflow run table
CREATE TABLE IF NOT EXISTS hyrex_workflow_run (
    id              UUID                        NOT NULL PRIMARY KEY,
    parent_id       UUID,
    workflow_name   VARCHAR                     NOT NULL,
    args            JSON                        NOT NULL,
    queue           VARCHAR                     NOT NULL,
    timeout_seconds INT                         DEFAULT NULL CHECK (timeout_seconds IS NULL OR timeout_seconds > 0),
    status          workflow_run_status         NOT NULL,
    scheduled_start TIMESTAMP WITH TIME ZONE,
    queued          TIMESTAMP WITH TIME ZONE,
    started         TIMESTAMP WITH TIME ZONE,
    finished        TIMESTAMP WITH TIME ZONE,
    last_heartbeat  TIMESTAMP WITH TIME ZONE,
    idempotency_key VARCHAR
);
"""

INSERT_WORKFLOW_RUN = """
INSERT INTO hyrex_workflow_run (
  id,
  parent_id,
  workflow_name,
  args,
  queue,
  timeout_seconds,
  status,
  queued,
  last_heartbeat,
  idempotency_key
)
VALUES ($1, NULL, $2, $3, $4, $5, 'running'::workflow_run_status, now(), now(), $6)
RETURNING id;
"""

# Returns (workflow_id, workflow_run_status)
SET_WORKFLOW_RUN_STATUS_BASED_ON_TASK_RUNS = """
    WITH latest_attempts AS (
        -- Get the latest attempt for each durable_id
        SELECT DISTINCT ON (durable_id)
            durable_id,
            workflow_run_id,
            status,
            attempt_number,
            max_retries
        FROM hyrex_task_run
        ORDER BY durable_id, attempt_number DESC
    ),
         workflow_statuses AS (
             SELECT w.id AS workflow_run_id,
                    CASE
                        -- If any task has failed on its final attempt, workflow is failed
                        WHEN EXISTS (
                            SELECT 1
                            FROM latest_attempts
                            WHERE latest_attempts.workflow_run_id = w.id
                              AND status = 'failed'
                              AND attempt_number >= max_retries
                        ) THEN 'failed'::workflow_run_status

                        -- If any task is still in progress, workflow is running
                        WHEN EXISTS (
                            SELECT 1
                            FROM latest_attempts
                            WHERE latest_attempts.workflow_run_id = w.id
                              AND status IN ('running', 'queued', 'waiting', 'up_for_cancel')
                        ) THEN 'running'::workflow_run_status

                        -- If all tasks are either success or skipped, workflow is success
                        WHEN NOT EXISTS (
                            SELECT 1
                            FROM latest_attempts
                            WHERE latest_attempts.workflow_run_id = w.id
                              AND status NOT IN ('success', 'skipped')
                        ) THEN 'success'::workflow_run_status

                        -- Handle other states (lost, canceled)
                        WHEN EXISTS (
                            SELECT 1
                            FROM latest_attempts
                            WHERE latest_attempts.workflow_run_id = w.id
                              AND status IN ('lost', 'canceled')
                        ) THEN 'failed'::workflow_run_status

                        -- Otherwise, keep current status
                        ELSE w.status
                        END AS new_status
             FROM hyrex_workflow_run w
             WHERE w.id = $1 -- Specific workflow
               AND w.status NOT IN ('success', 'failed') -- Only if not in terminal state
         )
    UPDATE hyrex_workflow_run w
    SET status   = ws.new_status,
        finished = CASE
                       WHEN ws.new_status IN ('success', 'failed')
                           THEN CURRENT_TIMESTAMP
                       ELSE w.finished
            END
    FROM workflow_statuses ws
    WHERE w.id = ws.workflow_run_id
    RETURNING w.id, w.status;
"""

ADVANCE_WORKFLOW_RUN = """
    WITH latest_attempts AS (
        -- Get the latest attempt for each durable_id
        SELECT DISTINCT ON (durable_id) durable_id,
                                        status
        FROM hyrex_task_run
        ORDER BY durable_id, attempt_number DESC),
         tasks_ready_to_queue AS (
             -- Find waiting tasks where all dependencies are successful
             SELECT t.id
             FROM hyrex_task_run t
             WHERE t.workflow_run_id = $1
               AND t.status = 'waiting'
               AND (
                 -- Handle both cases: no dependencies, or all dependencies are successful
                 t.workflow_dependencies IS NULL
                     OR NOT EXISTS (
                     -- Check if there are any dependencies that aren't successful
                     SELECT 1
                     FROM unnest(t.workflow_dependencies) AS dep_durable_id
                              LEFT JOIN latest_attempts la ON la.durable_id = dep_durable_id
                     WHERE la.status IS NULL
                        OR la.status != 'success')
                 ))
    UPDATE hyrex_task_run
    SET status = 'queued'
    WHERE id IN (SELECT id FROM tasks_ready_to_queue);
"""

GET_WORKFLOW_RUN_ARGS = """
    SELECT args
    FROM hyrex_workflow_run
    WHERE id = $1;
"""
