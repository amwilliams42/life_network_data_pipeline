{{ config(materialized='table') }}

with
    run_crew as (
        select *
        from {{ ref('int_run_crew_assignments') }}
        where service_date >= current_date - interval '5 weeks'
        and service_date < current_date + interval '1 day'
    ),

    run_metrics as (
        select distinct on (leg_id, source_database)
            -- Run identifiers
            run_number,
            leg_id,
            pcr_number,
            date_of_service,

            -- Run details
            calltype_name,
            source_name,
            level_of_service,
            market,
            priority_id,
            transport_priority_id,
            emergency,
            trip_status,
            reason_for_transport,

            -- Timestamps
            service_date,
            pickup_time,
            call_started_date,
            assigned_time,
            acknowledged_time,
            enroute_time,
            at_scene_time,
            transporting_time,
            at_destination_time,
            clear_time,

            -- Region (same logic as time_on_task_daily)
            case
                when source_database = 'il' then 'il'
                when source_database = 'mi' then 'mi'
                when source_database = 'tn' and (cost_center_name like 'Memp%' or cost_center_name like 'Miss%') then 'tn_memphis'
                when source_database = 'tn' and cost_center_name like 'Nash%' then 'tn_nashville'
                else source_database
            end as region,

            -- Time on Task metrics (in minutes and hours)
            time_on_task_minutes,
            time_on_task_minutes / 60.0 as time_on_task_hours,

            time_on_task_excl_transport_minutes,
            time_on_task_excl_transport_minutes / 60.0 as time_on_task_excl_transport_hours,

            -- Component times (in minutes)
            scene_time_minutes,
            transport_time_minutes,
            destination_to_clear_minutes as destination_time_minutes,
            response_time_minutes,
            enroute_to_scene_minutes,
            ack_to_enroute_minutes,

            -- Component times (in hours)
            scene_time_minutes / 60.0 as scene_time_hours,
            transport_time_minutes / 60.0 as transport_time_hours,
            destination_to_clear_minutes / 60.0 as destination_time_hours,

            -- Crew/shift information
            shift_assignment_id,
            assignment_id,
            user_id,
            first_name,
            last_name,
            assigned_name,
            employee_num,
            job_title,

            -- Shift timing
            shift_date,
            shift_start,
            shift_end,
            scheduled_hours,

            -- Unit information
            unit_id,
            unit_name,
            position_slot,

            -- Cost center
            cost_center_id,
            cost_center_name,
            special_event,

            -- Flags
            is_training,
            assignment_status,

            -- Source
            source_database,

            -- Metadata
            run_created_timestamp,
            run_modified_timestamp,

            -- Calculated percentages (if crew is assigned)
            case
                when scheduled_hours > 0
                then (time_on_task_minutes / 60.0 / scheduled_hours) * 100
            end as run_tot_pct_of_shift,

            -- Time of day analytics
            extract(hour from assigned_time) as assigned_hour,
            extract(dow from service_date) as day_of_week,
            to_char(service_date, 'Day') as day_name,
            date_trunc('week', service_date)::date as week_start,
            date_trunc('month', service_date)::date as month_start

        from run_crew
        where calltype_name in ('ALS', 'BLS', 'CCT')
        order by leg_id, source_database, shift_assignment_id
    )

select * from run_metrics
