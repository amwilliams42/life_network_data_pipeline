{{ config(materialized='view') }}

WITH
    time_points AS (
        SELECT DISTINCT
            shift_name,
            shift_start AS time_point
        FROM {{ref('stg_schedule')}}
        UNION
        SELECT DISTINCT
            shift_name,
            shift_end AS time_point
        FROM {{ref('stg_schedule')}}
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
            COALESCE(s.name, 'OPEN') AS name
        FROM
            intervals i
            JOIN {{ref('stg_schedule')}} s on i.shift_name = s.shift_name
            AND s.shift_start <= i.start_time
            AND s.shift_end > i.start_time
        WHERE
            i.end_time IS NOT NULL
    )
SELECT
    shift_name,
    ARRAY_AGG(
        name::text
        ORDER BY
            name
    ) AS crew_ids,
    start_time,
    end_time
FROM
    active_crews
GROUP BY
    shift_name,
    start_time,
    end_time
ORDER BY
    shift_name,
    start_time