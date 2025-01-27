CREATE_MATERIALIZED_VIEW_FINISHED_STATS = """
CREATE MATERIALIZED VIEW hystats_finished_by_time AS
WITH time_buckets AS (
  SELECT 
    date_bin('5 seconds'::interval, finished, TIMESTAMP WITH TIME ZONE '2001-01-01') as time_bucket,
    task_name,
    status,
    COUNT(*) as task_count
  FROM hyrex_task_run
  WHERE 
    finished IS NOT NULL
  GROUP BY 
    date_bin('5 seconds'::interval, finished, TIMESTAMP WITH TIME ZONE '2001-01-01'),
    task_name,
    status
)
SELECT 
  time_bucket,
  task_name,
  status,
  task_count,
  ROUND(100.0 * task_count / SUM(task_count) OVER (
    PARTITION BY time_bucket, task_name
  ), 2) as percentage_by_task,
  ROUND(100.0 * task_count / SUM(task_count) OVER (
    PARTITION BY time_bucket
  ), 2) as percentage_overall
FROM time_buckets
ORDER BY 
  time_bucket,
  task_name,
  status;

-- Create a unique index required for concurrent refresh
CREATE UNIQUE INDEX hystats_finished_by_time_unique_idx 
ON hystats_finished_by_time(time_bucket, task_name, status);

-- Create additional indexes for query performance
CREATE INDEX idx_task_stats_time_bucket 
ON hystats_finished_by_time(time_bucket);

CREATE INDEX idx_task_stats_task_name 
ON hystats_finished_by_time(task_name);
"""

CREATE_GET_FRESH_STATS_FUNCTION = """
CREATE OR REPLACE FUNCTION get_fresh_task_stats(lookback_period INTERVAL = '1 hour'::INTERVAL)
RETURNS TABLE (
    time_bucket timestamp with time zone,
    task_name varchar,
    status STATUS_ENUM,
    task_count bigint,
    percentage_by_task numeric,
    percentage_overall numeric
) AS $$
BEGIN
    -- Check if refresh is needed by looking at the most recent refresh event
    IF NOT EXISTS (
        SELECT 1
        FROM hyrex_system_logs
        WHERE event_name = 'hystats_finished_by_time_refresh'
        AND timestamp > NOW() - INTERVAL '5 seconds'
    ) THEN
        -- Refresh the view
        REFRESH MATERIALIZED VIEW CONCURRENTLY hystats_finished_by_time;
        
        -- Log the refresh event
        INSERT INTO hyrex_system_logs (
            id,
            timestamp,
            event_name,
            event_body
        ) VALUES (
            gen_random_uuid(),
            NOW(),
            'hystats_finished_by_time_refresh',
            '{}'::jsonb
        );
    END IF;

    -- Return the data filtered by lookback period
    RETURN QUERY 
    SELECT * FROM hystats_finished_by_time mv
    WHERE mv.time_bucket > NOW() - lookback_period
    ORDER BY mv.time_bucket DESC;
END;
$$ LANGUAGE plpgsql;
"""

CREATE_MATERIALIZED_VIEW_QUEUED_STATS = """
CREATE MATERIALIZED VIEW hystats_queued_by_time AS
WITH time_buckets AS (
  SELECT 
    date_bin('5 seconds'::interval, queued, TIMESTAMP WITH TIME ZONE '2001-01-01') as time_bucket,
    task_name,
    status,
    COUNT(*)::numeric / 5 as tasks_per_second
  FROM hyrex_task_run
  WHERE 
    queued IS NOT NULL
  GROUP BY 
    date_bin('5 seconds'::interval, queued, TIMESTAMP WITH TIME ZONE '2001-01-01'),
    task_name,
    status
)
SELECT 
  time_bucket,
  task_name,
  status,
  ROUND(tasks_per_second, 2) as tasks_per_second,
  ROUND(100.0 * tasks_per_second / SUM(tasks_per_second) OVER (
    PARTITION BY time_bucket, task_name
  ), 2) as percentage_by_task,
  ROUND(100.0 * tasks_per_second / SUM(tasks_per_second) OVER (
    PARTITION BY time_bucket
  ), 2) as percentage_overall
FROM time_buckets
ORDER BY 
  time_bucket,
  task_name,
  status;

-- Create a unique index required for concurrent refresh
CREATE UNIQUE INDEX hystats_queued_by_time_unique_idx 
ON hystats_queued_by_time(time_bucket, task_name, status);

-- Create additional indexes for query performance
CREATE INDEX idx_queued_stats_time_bucket 
ON hystats_queued_by_time(time_bucket);

CREATE INDEX idx_queued_stats_task_name 
ON hystats_queued_by_time(task_name);
"""

CREATE_GET_FRESH_QUEUED_STATS_FUNCTION = """
CREATE OR REPLACE FUNCTION get_fresh_queued_stats(lookback_period INTERVAL = '1 hour'::INTERVAL)
RETURNS TABLE (
    time_bucket timestamp with time zone,
    task_name varchar,
    status STATUS_ENUM,
    tasks_per_second numeric,
    percentage_by_task numeric,
    percentage_overall numeric
) AS $$
BEGIN
    -- Check if refresh is needed by looking at the most recent refresh event
    IF NOT EXISTS (
        SELECT 1
        FROM hyrex_system_logs
        WHERE event_name = 'hystats_queued_by_time_refresh'
        AND timestamp > NOW() - INTERVAL '5 seconds'
    ) THEN
        -- Refresh the view
        REFRESH MATERIALIZED VIEW CONCURRENTLY hystats_queued_by_time;
        
        -- Log the refresh event
        INSERT INTO hyrex_system_logs (
            id,
            timestamp,
            event_name,
            event_body
        ) VALUES (
            gen_random_uuid(),
            NOW(),
            'hystats_queued_by_time_refresh',
            '{}'::jsonb
        );
    END IF;

    -- Return the data filtered by lookback period
    RETURN QUERY 
    SELECT * FROM hystats_queued_by_time mv
    WHERE mv.time_bucket > NOW() - lookback_period
    ORDER BY mv.time_bucket DESC;
END;
$$ LANGUAGE plpgsql;
"""

CREATE_HISTORICAL_TASK_STATUS_COUNTS = """
    CREATE TABLE IF NOT EXISTS hyrex_stats_task_status_counts
    (
        timepoint     TIMESTAMP WITH TIME ZONE PRIMARY KEY,
        queued        INTEGER,
        running       INTEGER,
        waiting       INTEGER,
        failed        INTEGER,
        success       INTEGER,
        total         INTEGER,
        queued_delta  INTEGER,
        success_delta INTEGER
    );
"""

FILL_HISTORICAL_TASK_STATUS_COUNTS_TABLE = """
WITH RECURSIVE timepoints AS (
    -- 1) Start from the larger of:
    --    - The last known timepoint from the stats table (if any)
    --    - 10 minutes ago (rounded to a 15s boundary)
    SELECT GREATEST(
               COALESCE(
                   (SELECT MAX(timepoint) FROM hyrex_stats_task_status_counts),
                   date_bin(
                       INTERVAL '15 seconds',
                       now() - INTERVAL '10 minutes',
                       TIMESTAMP '2000-01-01 00:00:00+00'
                   )
               ),
               date_bin(
                   INTERVAL '15 seconds',
                   now() - INTERVAL '10 minutes',
                   TIMESTAMP '2000-01-01 00:00:00+00'
               )
           ) + INTERVAL '15 seconds' AS timepoint

    UNION ALL

    -- 2) Keep adding 15 seconds, up to 'now' (also rounded to a 15s boundary)
    SELECT timepoint + INTERVAL '15 seconds'
    FROM timepoints
    WHERE timepoint < date_bin(
        INTERVAL '15 seconds',
        now(),
        TIMESTAMP '2000-01-01 00:00:00+00'
    )
),

 -- 2) For each timepoint, count the tasks in each status
 queue_counts AS (
     SELECT
         t.timepoint,
         COUNT(CASE
                   WHEN he.queued <= t.timepoint
                       AND (he.started IS NULL OR he.started > t.timepoint)
                       THEN 1 END) AS queued,
         COUNT(CASE
                   WHEN he.started <= t.timepoint
                       AND (he.finished IS NULL OR he.finished > t.timepoint)
                       AND he.status = 'running'
                       THEN 1 END) AS running,
         COUNT(CASE
                   WHEN he.status = 'waiting'
                       AND he.queued <= t.timepoint
                       AND (he.finished IS NULL OR he.finished > t.timepoint)
                       THEN 1 END) AS waiting,
         COUNT(CASE
                   WHEN he.status IN ('failed','up_for_retry')
                       AND he.finished <= t.timepoint
                       THEN 1 END) AS failed,
         COUNT(CASE
                   WHEN he.status = 'success'
                       AND he.finished <= t.timepoint
                       THEN 1 END) AS success
     FROM timepoints t
              LEFT JOIN hyrex_task_run he
                        ON (
                            -- Include tasks that existed during this timepoint
                            he.queued <= t.timepoint
                                AND (
                                -- Either they're still in the system
                                he.finished IS NULL
                                    OR
                                    -- Or they finished after this timepoint
                                he.finished > t.timepoint
                                    OR
                                    -- Or they failed/retry/succeeded at this exact timepoint
                                (
                                    he.status IN ('failed','up_for_retry','success')
                                        AND he.finished <= t.timepoint
                                    )
                                )
                            )
     GROUP BY t.timepoint
 ),

 -- 3) Compute deltas in a separate CTE so we can filter rows with NULL deltas
 final_counts AS (
     SELECT
         timepoint,
         queued,
         running,
         waiting,
         failed,
         success,
         (queued + running + waiting + failed) AS total,
         (queued - LAG(queued, 1) OVER (ORDER BY timepoint))   AS queued_delta,
         (success - LAG(success, 1) OVER (ORDER BY timepoint)) AS success_delta
     FROM queue_counts
 )

-- 4) Insert new rows, skipping those where deltas are NULL
 INSERT INTO hyrex_stats_task_status_counts
 SELECT
     timepoint,
     queued,
     running,
     waiting,
     failed,
     success,
     total,
     queued_delta,
     success_delta
 FROM final_counts
 WHERE queued_delta IS NOT NULL
    AND success_delta IS NOT NULL
 ON CONFLICT (timepoint) DO NOTHING;
"""
