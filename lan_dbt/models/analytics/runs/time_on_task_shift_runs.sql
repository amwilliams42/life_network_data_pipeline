{{ config(materialized='table') }}

/*
    Drill-down view for analyzing individual runs within a specific shift.
    This view provides detailed run-level information that can be filtered
    by crew member, shift assignment, or date to see the specific runs
    that contributed to their Time on Task metrics.
*/

with
    run_metrics as (
        select * from {{ ref('time_on_task_run_metrics') }}
    )

select
    -- Shift/crew identifiers for filtering
    assignment_id,
    shift_assignment_id,
    user_id,
    assigned_name,
    first_name,
    last_name,
    employee_num,
    shift_date,
    unit_name,

    -- Run identifiers
    run_number,
    leg_id,
    pcr_number,
    date_of_service,
    service_date,

    -- Run classification
    calltype_name,
    source_name,
    level_of_service,
    market,
    priority_id,
    emergency,
    trip_status,
    is_training,

    -- Complete timeline
    call_started_date,
    assigned_time,
    acknowledged_time,
    enroute_time,
    at_scene_time,
    transporting_time,
    at_destination_time,
    clear_time,

    -- Key metrics (in minutes for precision)
    time_on_task_minutes,
    time_on_task_excl_transport_minutes,
    scene_time_minutes,
    transport_time_minutes,
    destination_time_minutes,
    response_time_minutes,
    enroute_to_scene_minutes,
    ack_to_enroute_minutes,

    -- Also provide in hours for easier reading
    time_on_task_hours,
    time_on_task_excl_transport_hours,
    scene_time_hours,
    transport_time_hours,
    destination_time_hours,

    -- Percentage of this run relative to the shift
    run_tot_pct_of_shift,

    -- Shift context
    shift_start,
    shift_end,
    scheduled_hours,

    -- Cost center
    cost_center_name,

    -- Source
    source_database,

    -- Time of day analytics
    assigned_hour,
    day_of_week,
    day_name,

    -- Sort helper (order runs chronologically within a shift)
    row_number() over (
        partition by assignment_id, shift_date
        order by assigned_time
    ) as run_sequence_in_shift,

    -- Metadata
    run_created_timestamp,
    run_modified_timestamp

from run_metrics
order by shift_date desc, assigned_name, assigned_time
