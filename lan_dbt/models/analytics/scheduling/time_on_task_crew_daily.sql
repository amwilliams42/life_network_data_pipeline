{{ config(materialized='view') }}

with
    shift_metrics as (
        select * from {{ ref('int_shift_metrics') }}
    )

select
    -- Crew identifiers
    assignment_id,
    shift_assignment_id,
    user_id,
    first_name,
    last_name,
    assigned_name,
    employee_num,
    job_title,

    -- Shift details
    shift_date,
    shift_start,
    shift_end,
    scheduled_hours,
    clock_in_time,
    clock_out_time,
    actual_hours_worked,

    -- Unit information
    unit_id,
    unit_name,
    position_slot,
    required_qualification,

    -- Cost center
    cost_center_id,
    cost_center_name,

    -- Flags
    is_training,
    assignment_status,

    -- Source
    source_database,

    -- Run counts
    total_runs,
    total_run_numbers,

    -- Time on Task metrics
    total_time_on_task_hours,
    total_time_on_task_excl_transport_hours,
    time_on_task_pct,
    time_on_task_excl_transport_pct,

    -- Component time totals (in hours)
    total_scene_time_hours,
    total_destination_time_hours,
    total_transport_time_hours,

    -- Average times per run (in minutes)
    avg_time_on_task_minutes,
    avg_time_on_task_excl_transport_minutes,
    avg_scene_time_minutes,
    avg_destination_time_minutes,
    avg_transport_time_minutes,
    avg_response_time_minutes,

    -- Unit Hour Utilization
    unit_hour_utilization,

    -- Active time
    active_shift_hours,
    first_run_assigned_time,
    last_run_clear_time,

    -- Efficiency metrics
    case
        when active_shift_hours > 0
        then (total_time_on_task_hours / active_shift_hours) * 100
    end as active_time_efficiency_pct,

    case
        when actual_hours_worked > 0
        then (total_time_on_task_hours / actual_hours_worked) * 100
    end as actual_hours_efficiency_pct,

    -- Date analytics
    extract(dow from shift_date) as day_of_week,
    to_char(shift_date, 'Day') as day_name,
    date_trunc('week', shift_date)::date as week_start,
    date_trunc('month', shift_date)::date as month_start

from shift_metrics
order by shift_date desc, assigned_name
