{{ config(materialized='view') }}

WITH
    time_points AS (
        SELECT DISTINCT
            unit_name,
            shift_start AS time_point
        FROM {{ref('stg_schedule')}}
        WHERE unit_name IS NOT NULL 
        AND shift_start IS NOT NULL
        UNION
        SELECT DISTINCT
            unit_name,
            shift_end AS time_point
        FROM {{ref('stg_schedule')}}
        WHERE unit_name IS NOT NULL 
        AND shift_end IS NOT NULL
    ),
    intervals AS (
        SELECT
            unit_name,
            time_point as start_time,
            LEAD (time_point) OVER (
                PARTITION BY unit_name
                ORDER BY time_point
            ) AS end_time
        FROM time_points
    ),
    active_crews AS (
        SELECT
            i.unit_name,
            i.start_time,
            i.end_time,
            s.date_line,
            COALESCE(s.assigned_name, 'OPEN') AS crew_member_name,
            s.scheduled_hours,
            s.open_hours,
            s.hours_difference AS worked_hours,
            s.source_database,
            s.cost_center_id,
            s.cost_center_name,
            s.first_name,
            s.last_name,
            s.employee_num,
            s.job_title,
            s.assignment_status,
            -- Calculate the duration of this interval in hours
            ROUND(
                (EXTRACT(EPOCH FROM i.end_time) - EXTRACT(EPOCH FROM i.start_time)) / 3600.0, 
                2
            ) AS interval_duration_hours
        FROM intervals i
        INNER JOIN {{ref('stg_schedule')}} s 
            ON i.unit_name = s.unit_name
            AND s.shift_start <= i.start_time
            AND s.shift_end > i.start_time
            AND s.shift_end >= i.end_time
        WHERE i.end_time IS NOT NULL
        AND i.start_time < i.end_time  -- Ensure valid time intervals
    )
SELECT
    unit_name,
    start_time,
    end_time,
    date_line,
    cost_center_id,
    cost_center_name,
    
    -- Crew information
    ARRAY_AGG(crew_member_name ORDER BY crew_member_name) AS crew_names,
    ARRAY_AGG(
        CASE 
            WHEN first_name IS NOT NULL AND last_name IS NOT NULL 
            THEN CONCAT(first_name, ' ', last_name)
            ELSE crew_member_name 
        END 
        ORDER BY crew_member_name
    ) AS crew_full_names,
    COUNT(*) AS crew_count,
    
    -- Assignment status summary
    STRING_AGG(DISTINCT assignment_status, ', ' ORDER BY assignment_status) AS assignment_statuses,
    
    -- Hours calculations with proper rounding
    ROUND(COUNT(*) * MAX(interval_duration_hours), 2) AS total_scheduled_hours,
    ROUND(
        SUM(CASE WHEN crew_member_name = 'OPEN' THEN interval_duration_hours ELSE 0 END), 
        2
    ) AS total_open_hours,
    ROUND(SUM(COALESCE(worked_hours, 0)), 2) AS total_worked_hours,
    ROUND(SUM(COALESCE(scheduled_hours, 0)), 2) AS total_individual_scheduled_hours,
    
    -- Utilization metrics
    ROUND(
        CASE 
            WHEN SUM(COALESCE(scheduled_hours, 0)) > 0 
            THEN (SUM(COALESCE(worked_hours, 0)) / SUM(COALESCE(scheduled_hours, 0))) * 100 
            ELSE 0 
        END, 
        1
    ) AS utilization_percentage,
    
    -- Data source tracking
    STRING_AGG(DISTINCT source_database, ', ' ORDER BY source_database) AS source_databases,
    
    -- Add metadata
    MAX(interval_duration_hours) AS interval_duration_hours
FROM active_crews
GROUP BY
    unit_name,
    cost_center_id,
    cost_center_name,
    date_line,
    start_time,
    end_time
HAVING COUNT(*) > 0  -- Ensure we have at least one crew member
ORDER BY
    unit_name,
    start_time