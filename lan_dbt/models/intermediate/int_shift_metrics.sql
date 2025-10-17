{{ config(materialized='view') }}

with
    run_crew as (
        select * from {{ ref('int_run_crew_assignments') }}
    ),

    shift_aggregations as (
        select
            -- Shift identifiers
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

            -- Aggregated run metrics
            count(distinct leg_id) as total_runs,
            count(distinct run_number) as total_run_numbers,

            -- Time on Task aggregations (convert minutes to hours)
            sum(time_on_task_minutes) / 60.0 as total_time_on_task_hours,
            sum(time_on_task_excl_transport_minutes) / 60.0 as total_time_on_task_excl_transport_hours,
            sum(scene_time_minutes) / 60.0 as total_scene_time_hours,
            sum(destination_to_clear_minutes) / 60.0 as total_destination_time_hours,
            sum(transport_time_minutes) / 60.0 as total_transport_time_hours,

            -- Average times (in minutes)
            avg(time_on_task_minutes) as avg_time_on_task_minutes,
            avg(time_on_task_excl_transport_minutes) as avg_time_on_task_excl_transport_minutes,
            avg(scene_time_minutes) as avg_scene_time_minutes,
            avg(destination_to_clear_minutes) as avg_destination_time_minutes,
            avg(transport_time_minutes) as avg_transport_time_minutes,
            avg(response_time_minutes) as avg_response_time_minutes,

            -- First and last run times
            min(assigned_time) as first_run_assigned_time,
            max(clear_time) as last_run_clear_time

        from run_crew
        where assignment_id is not null
        group by
            assignment_id,
            shift_assignment_id,
            user_id,
            first_name,
            last_name,
            assigned_name,
            employee_num,
            job_title,
            shift_date,
            shift_start,
            shift_end,
            scheduled_hours,
            clock_in_time,
            clock_out_time,
            actual_hours_worked,
            unit_id,
            unit_name,
            position_slot,
            required_qualification,
            cost_center_id,
            cost_center_name,
            is_training,
            assignment_status,
            source_database
    ),

    shift_metrics_calculated as (
        select
            *,

            -- Time on Task percentages (based on scheduled hours)
            case
                when scheduled_hours > 0
                then (total_time_on_task_hours / scheduled_hours)
            end as time_on_task_pct,

            case
                when scheduled_hours > 0
                then (total_time_on_task_excl_transport_hours / scheduled_hours)
            end as time_on_task_excl_transport_pct,

            -- Unit Hour Utilization (UHU) - runs per hour
            case
                when scheduled_hours > 0
                then total_runs::numeric / scheduled_hours
            end as unit_hour_utilization,

            -- Active time calculation (time from first assignment to last clear)
            case
                when first_run_assigned_time is not null and last_run_clear_time is not null
                then extract(epoch from (last_run_clear_time - first_run_assigned_time)) / 3600.0
            end as active_shift_hours

        from shift_aggregations
    )

select * from shift_metrics_calculated
