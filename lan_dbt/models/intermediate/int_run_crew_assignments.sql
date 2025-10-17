{{ config(materialized='view') }}

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

with
    run_timestamps as (
        select * from {{ ref('stg_run_timestamps') }}
    ),

    runs as (
        select * from {{ ref('stg_runs') }}
    ),

    -- Use macro to get valid cost centers for crew shifts
    {{ get_valid_cost_centers() }},

    schedule as (
        select * from {{ ref('stg_schedule') }}
    ),

{% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
    {{ suffix }}_leg_assignments as (
        select
            leg_id,
            shift_assignment_id,
            '{{ suffix }}' as source_database
        from {{ source(dataset, 'cad_trip_leg_shift_assignments') }}
    ){% if not loop.last %},{% endif %}
{% endfor %},

    all_leg_assignments as (
        select * from tn_leg_assignments
        union all
        select * from mi_leg_assignments
        union all
        select * from il_leg_assignments
    ),

    run_crew_base as (
        select
            -- Run identifiers
            r.run_number,
            r.leg_id,
            r.pcr_number,
            r.date_of_service,

            -- Run details
            r.calltype_name,
            r.source_name,
            r.level_of_service,
            r.market,
            r.priority_id,
            r.transport_priority_id,
            r.emergency,
            r.trip_status,
            r.last_status_id,
            r.reason_for_transport,

            -- Timestamps
            rt.service_date,
            rt.pickup_time,
            rt.call_started_date,
            rt.assigned_time,
            rt.acknowledged_time,
            rt.enroute_time,
            rt.at_scene_time,
            rt.transporting_time,
            rt.at_destination_time,
            rt.clear_time,
            rt.canceled_time,

            -- Duration calculations (in minutes)
            rt.scene_time_minutes,
            rt.transport_time_minutes,
            rt.destination_to_clear_minutes,
            rt.total_unit_time_minutes,
            rt.response_time_minutes,
            rt.enroute_to_scene_minutes,
            rt.ack_to_enroute_minutes,

            -- Time on Task calculations (in minutes)
            case
                when rt.assigned_time is not null and rt.clear_time is not null
                then extract(epoch from (rt.clear_time - rt.assigned_time)) / 60.0
            end as time_on_task_minutes,

            case
                when rt.scene_time_minutes is not null and rt.destination_to_clear_minutes is not null
                then rt.scene_time_minutes + rt.destination_to_clear_minutes
            end as time_on_task_excl_transport_minutes,

            -- Crew/shift assignment details
            la.shift_assignment_id,
            s.assignment_id,
            s.user_id,
            s.first_name,
            s.last_name,
            s.assigned_name,
            s.employee_num,
            s.job_title,

            -- Shift timing
            s.date_line as shift_date,
            s.shift_start,
            s.shift_end,
            s.scheduled_hours,
            s.clock_in_time,
            s.clock_out_time,
            s.hours_difference as actual_hours_worked,

            -- Unit information
            s.unit_id,
            s.unit_name,
            s.position_slot,
            s.required_qualification,

            -- Cost center
            s.cost_center_id,
            s.cost_center_name,

            -- Flags
            s.is_training,
            s.assignment_status,

            -- Source database
            r.source_database,

            -- Metadata
            r.created_timestamp as run_created_timestamp,
            r.modified_timestamp as run_modified_timestamp

        from runs as r
        inner join run_timestamps as rt
            on r.leg_id = rt.leg_id
            and r.source_database = rt.source_database
        left join all_leg_assignments as la
            on r.leg_id = la.leg_id
            and r.source_database = la.source_database
        left join schedule as s
            on la.shift_assignment_id = s.assignment_id
            and la.source_database = s.source_database
        -- Filter to only valid crew cost centers
        inner join valid_cost_centers as vcc
            on s.cost_center_id = vcc.cost_center_id
            and s.source_database = vcc.source_database
    )

select * from run_crew_base
where
    -- Filter out canceled runs
    canceled_time is null
    -- Ensure we have valid assigned and clear times for ToT calculations
    and assigned_time is not null
    and clear_time is not null
    -- Filter out runs with invalid timestamps (data quality issues)
    and clear_time > assigned_time  -- Clear must be after assigned
    and time_on_task_minutes > 0  -- Must be positive
    and time_on_task_minutes < 720  -- Less than 12 hours (reasonable max for a single run)
