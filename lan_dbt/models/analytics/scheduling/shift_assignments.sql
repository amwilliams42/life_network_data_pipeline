{{ config(materialized='table') }}

WITH
    time_points AS (
        SELECT DISTINCT
            shift_name,
            shift_start AS time_point
        FROM {{ref('schedule')}}
        UNION
        SELECT DISTINCT
            shift_name,
            shift_end AS time_point
        FROM {{ref('schedule')}}
    ),
    intervals AS (
        SELECT
            shift_name,
            time_point as start_time,
            LEAD (time_point) OVER (
                PARTITION BY
                    shift_name
                ORDER BY
                    time_point
            ) AS end_time
        FROM
            time_points
    ),
    active_crews AS (
        SELECT
            i.shift_name,
            i.start_time,
            i.end_time,
            s.date_line,
            COALESCE(s.name, 'OPEN') AS name,
            s.scheduled_length,
            s.open_hours,
            s.worked,
            s.source_database,
            s.cost_center_id,
            s.cost_center_name,
            -- Calculate the duration of this interval in hours
            (EXTRACT(EPOCH FROM i.end_time) - EXTRACT(EPOCH FROM i.start_time)) / 3600 AS interval_duration_hours
        FROM
            intervals i
            JOIN {{ref('schedule')}} s on i.shift_name = s.shift_name
            AND s.shift_start <= i.start_time
            AND s.shift_end > i.start_time
            AND s.shift_end >= i.end_time
        WHERE
            i.end_time IS NOT NULL
    )
SELECT
    shift_name,
    start_time,
    end_time,
    date_line,
    cost_center_id,
    cost_center_name,
    ARRAY_AGG(
        name::text
        ORDER BY
            name
    ) AS crew_names,
    -- Calculate total hours as interval duration × number of people
    COUNT(*) * MAX(interval_duration_hours) AS total_scheduled_length,
    -- Calculate open hours as interval duration × number of OPEN slots
    SUM(CASE WHEN name = 'OPEN' THEN interval_duration_hours ELSE 0 END) AS total_open_hours,
    -- For worked hours, sum the actual worked time (this may need adjustment based on your needs)
    SUM(COALESCE(worked, 0)) AS total_worked_hours,
    -- For source_database, get distinct values or pick one
    STRING_AGG(DISTINCT source_database, ', ' ORDER BY source_database) AS source_databases
FROM
    active_crews
GROUP BY
    shift_name,
    cost_center_id,
    cost_center_name,
    date_line,
    start_time,
    end_time
ORDER BY
    shift_name,
    start_time