CREATE_HISTORICAL_TASK_STATUS_COUNTS = """
    CREATE TABLE IF NOT EXISTS hyrex_stats_task_status_counts
    (
        timepoint     TIMESTAMP WITH TIME ZONE PRIMARY KEY,
        queued        INTEGER,
        running       INTEGER,
        waiting       INTEGER,
        failed        INTEGER,
        success       INTEGER,
        lost          INTEGER,
        total         INTEGER,
        queued_delta  INTEGER,
        success_delta INTEGER,
        failed_delta  INTEGER,
        lost_delta    INTEGER
    );

    CREATE INDEX IF NOT EXISTS idx_hstsc_timepoint
        ON hyrex_stats_task_status_counts(timepoint);
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
                   queue_counts AS (
                       SELECT
                           t.timepoint,
                           COUNT(
                                   CASE
                                       WHEN htr.queued <= t.timepoint
                                           AND (htr.started IS NULL OR htr.started > t.timepoint)
                                           THEN 1
                                       END
                           ) AS queued,
                           COUNT(
                                   CASE
                                       WHEN htr.started <= t.timepoint
                                           AND (htr.finished IS NULL OR htr.finished > t.timepoint)
                                           AND htr.status = 'running'
                                           THEN 1
                                       END
                           ) AS running,
                           COUNT(
                                   CASE
                                       WHEN htr.status = 'waiting'
                                           AND htr.queued <= t.timepoint
                                           AND (htr.finished IS NULL OR htr.finished > t.timepoint)
                                           THEN 1
                                       END
                           ) AS waiting,
                           COUNT(
                                   CASE
                                       WHEN htr.status IN ('failed')
                                           AND htr.finished <= t.timepoint
                                           THEN 1
                                       END
                           ) AS failed,
                           COUNT(
                                   CASE
                                       WHEN htr.status = 'success'
                                           AND htr.finished <= t.timepoint
                                           THEN 1
                                       END
                           ) AS success,
                           COUNT(
                                   CASE
                                       WHEN htr.status = 'lost'
                                           AND htr.finished <= t.timepoint
                                           THEN 1
                                       END
                           ) AS lost
                       FROM timepoints t
                                LEFT JOIN hyrex_task_run htr
                                          ON (
                                              -- Include tasks that existed during this timepoint
                                              htr.queued <= t.timepoint
                                                  AND (
                                                  -- Either they're still in the system
                                                  htr.finished IS NULL
                                                      OR
                                                      -- Or they finished after this timepoint
                                                  htr.finished > t.timepoint
                                                      OR
                                                      -- Or they failed/succeeded/lost at this exact timepoint
                                                  (
                                                      htr.status IN ('failed','success','lost')
                                                          AND htr.finished <= t.timepoint
                                                      )
                                                  )
                                              )
                       GROUP BY t.timepoint
                   ),
                   final_counts AS (
                       SELECT
                           timepoint,
                           queued,
                           running,
                           waiting,
                           failed,
                           success,
                           lost,
                           (queued + running + waiting + failed + lost) AS total,
                           (queued - LAG(queued, 1) OVER (ORDER BY timepoint))   AS queued_delta,
                           (success - LAG(success, 1) OVER (ORDER BY timepoint)) AS success_delta,
                           (failed - LAG(failed, 1) OVER (ORDER BY timepoint))   AS failed_delta,
                           (lost - LAG(lost, 1) OVER (ORDER BY timepoint))       AS lost_delta
                       FROM queue_counts
                   )
    INSERT INTO hyrex_stats_task_status_counts
    SELECT
        timepoint,
        queued,
        running,
        waiting,
        failed,
        success,
        lost,
        total,
        queued_delta,
        success_delta,
        failed_delta,
        lost_delta
    FROM final_counts
    WHERE queued_delta IS NOT NULL
      AND success_delta IS NOT NULL
      AND failed_delta IS NOT NULL
      AND lost_delta IS NOT NULL
    ON CONFLICT (timepoint) DO NOTHING;
"""
