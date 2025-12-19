{{ config(materialized='view') }}

with
    shift_metrics as (
        select * from {{ ref('int_shift_metrics') }}
        where is_training = false  -- Exclude training shifts
    ),

    weekly_aggregations as (
        select
            -- Crew identifiers
            user_id,
            employee_num,
            first_name,
            last_name,
            assigned_name,
            job_title,

            -- Week grouping (Sunday to Saturday)
            (date_trunc('week', shift_date + 1)::date - 1) as week_start,

            -- Source
            source_database,

            -- Total calls ran in the week
            sum(total_runs) as total_calls_ran,
            sum(total_run_numbers) as total_run_numbers,

            -- Total hours for the week
            sum(scheduled_hours) as total_scheduled_hours,
            sum(actual_hours_worked) as total_actual_hours_worked,
            sum(total_time_on_task_hours) as total_time_on_task_hours,
            sum(total_time_on_task_excl_transport_hours) as total_time_on_task_excl_transport_hours,

            -- Weighted averages for time metrics (weighted by number of runs per shift)
            -- This gives a true average across all runs in the week
            sum(avg_time_on_task_minutes * total_runs) / nullif(sum(total_runs), 0) as avg_time_on_task_minutes,
            sum(avg_scene_time_minutes * total_runs) / nullif(sum(total_runs), 0) as avg_scene_time_minutes,
            sum(avg_response_time_minutes * total_runs) / nullif(sum(total_runs), 0) as avg_response_time_minutes,
            sum(avg_transport_time_minutes * total_runs) / nullif(sum(total_runs), 0) as avg_transport_time_minutes,
            sum(avg_destination_time_minutes * total_runs) / nullif(sum(total_runs), 0) as avg_destination_time_minutes,

            -- Average UHU across shifts in the week
            avg(unit_hour_utilization) as avg_daily_uhu,

            -- Count of shifts worked in the week
            count(*) as shifts_worked,

            -- Active time totals
            sum(active_shift_hours) as total_active_hours,

            -- Track which cost centers the crew member worked in
            string_agg(distinct cost_center_name, ', ' order by cost_center_name) as cost_centers_worked

        from shift_metrics
        group by
            user_id,
            employee_num,
            first_name,
            last_name,
            assigned_name,
            job_title,
            (date_trunc('week', shift_date + 1)::date - 1),
            source_database
    )

select
    -- Crew identifiers
    user_id,
    employee_num,
    first_name,
    last_name,
    assigned_name,
    job_title,

    -- Week information (Sunday to Saturday)
    week_start,
    (week_start + interval '6 days')::date as week_end,
    shifts_worked,

    -- Cost centers
    cost_centers_worked,

    -- Source
    source_database,

    -- Total calls ran in the week
    total_calls_ran,
    total_run_numbers,

    -- Average time on task (minutes)
    avg_time_on_task_minutes,

    -- Average time on scene (minutes)
    avg_scene_time_minutes,

    -- Average response time (minutes)
    avg_response_time_minutes,

    -- Average transport time (minutes)
    avg_transport_time_minutes,

    -- Average destination time (minutes)
    avg_destination_time_minutes,

    -- Unit Hour Utilization (UHU) - two calculations
    avg_daily_uhu,  -- Average of daily UHU values
    case
        when total_scheduled_hours > 0
        then total_calls_ran::numeric / total_scheduled_hours
    end as weekly_uhu,  -- Total calls / total scheduled hours for the week

    -- Hours totals
    total_scheduled_hours,
    total_actual_hours_worked,
    total_time_on_task_hours,
    total_time_on_task_excl_transport_hours,
    total_active_hours,

    -- Time on task percentage
    case
        when total_scheduled_hours > 0
        then (total_time_on_task_hours / total_scheduled_hours) * 100
    end as time_on_task_pct,

    case
        when total_scheduled_hours > 0
        then (total_time_on_task_excl_transport_hours / total_scheduled_hours) * 100
    end as time_on_task_excl_transport_pct,

    -- Efficiency metrics
    case
        when total_active_hours > 0
        then (total_time_on_task_hours / total_active_hours) * 100
    end as active_time_efficiency_pct,

    case
        when total_actual_hours_worked > 0
        then (total_time_on_task_hours / total_actual_hours_worked) * 100
    end as actual_hours_efficiency_pct

from weekly_aggregations
order by week_start desc, assigned_name