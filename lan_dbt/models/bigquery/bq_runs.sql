{{ config(materialized='table') }}

/*
    BigQuery Runs Export

    Comprehensive run/transport-level detail for PowerBI reporting.
    No date filtering - includes all available historical data.

    Join to bq_users via user_id + source_database for employee details.

    Key metrics calculable in DAX:
    - Total transports by region/level of service
    - Time on task (assigned to clear)
    - Response time (call to scene)
    - On-time percentage
    - Trips >50 miles
    - Flight crew transports
    - UHU (unit hour utilization)
*/

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

WITH
runs AS (
    SELECT * FROM {{ ref('stg_runs') }}
),

run_timestamps AS (
    SELECT * FROM {{ ref('stg_run_timestamps') }}
),

run_locations AS (
    SELECT * FROM {{ ref('stg_run_locations') }}
),

run_cancels AS (
    SELECT * FROM {{ ref('stg_run_cancels') }}
),

-- Get crew assignments for each run
{% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
{{ suffix }}_leg_assignments AS (
    SELECT
        leg_id,
        shift_assignment_id,
        '{{ suffix }}' AS source_database
    FROM {{ source(dataset, 'cad_trip_leg_shift_assignments') }}
){% if not loop.last %},{% endif %}
{% endfor %},

all_leg_assignments AS (
    SELECT * FROM tn_leg_assignments
    UNION ALL
    SELECT * FROM mi_leg_assignments
    UNION ALL
    SELECT * FROM il_leg_assignments
),

-- Pay periods for date context
pay_periods AS (
    SELECT * FROM {{ ref('pay_periods') }}
),

runs_enriched AS (
    SELECT
        -- Primary identifiers
        r.run_number,
        r.leg_id,
        r.pcr_number,
        r.source_database,

        -- Region: split TN into Memphis and Nashville
        CASE
            WHEN r.source_database = 'il' THEN 'il'
            WHEN r.source_database = 'mi' THEN 'mi'
            WHEN r.source_database = 'tn' AND r.market IN ('Memphis', 'Mississippi', 'Event - MEMP', 'Event - MISS') THEN 'tn_memphis'
            WHEN r.source_database = 'tn' AND r.market IN ('Nashville', 'Event - NASH') THEN 'tn_nashville'
            ELSE r.source_database
        END AS region,

        -- Division: Illinois, Michigan, Memphis, Nashville (accomodate IL Stupidity with markets)

        CASE
            WHEN r.source_database = 'il' THEN 'il'
            WHEN r.source_database = 'mi' THEN 'mi'
            WHEN r.source_database = 'tn' AND r.market IN ('Memphis', 'Mississippi') THEN 'tn_memphis'
            WHEN r.source_database = 'tn' AND r.market IN ('Nashville') THEN 'tn_nashville'
            ELSE null
        end as division,

        -- Date and time
        rt.service_date,
        rt.pickup_time,
        EXTRACT(dow FROM rt.service_date) AS day_of_week,
        TO_CHAR(rt.service_date, 'Day') AS day_name,
        EXTRACT(hour FROM rt.pickup_time) AS pickup_hour,
        DATE_TRUNC('week', rt.service_date)::date AS week_start,
        DATE_TRUNC('month', rt.service_date)::date AS month_start,

        -- Pay period context
        pp.pay_period_year,
        pp.pay_period_number,
        pp.start_date AS pay_period_start,
        pp.end_date AS pay_period_end,
        CASE
            WHEN rt.service_date >= (SELECT start_date FROM {{ ref('pay_periods') }} WHERE start_date <= CURRENT_DATE ORDER BY start_date DESC LIMIT 1)
                 AND rt.service_date <= (SELECT end_date FROM {{ ref('pay_periods') }} WHERE start_date <= CURRENT_DATE ORDER BY start_date DESC LIMIT 1)
            THEN true ELSE false
        END AS is_current_pay_period,

        -- Run details
        r.calltype_name,
        r.level_of_service,
        r.market,
        r.source_name,
        r.reason_for_transport,
        r.priority_id,
        r.transport_priority_id,
        r.emergency,
        r.trip_status,
        r.last_status_id,

        -- Run outcome (ran/turned/cancelled)
        CASE
            WHEN r.last_status_id > 0 THEN 'ran'
            WHEN r.last_status_id < 0 AND c.lost_call_status IS NOT NULL THEN 'turned'
            WHEN r.last_status_id < 0 AND c.lost_call_status IS NULL THEN 'cancelled'
            WHEN r.last_status_id IS NULL THEN 'cancelled'
            ELSE 'unknown'
        END AS run_outcome,

        -- Cancellation details (if applicable)
        c.cancel_reason_name,
        c.canceled_by,
        c.lost_call_status,

        -- Core EMS Timeline Timestamps
        rt.call_started_date,
        rt.assigned_time,
        rt.acknowledged_time,
        rt.enroute_time,
        rt.at_scene_time,
        rt.transporting_time,
        rt.at_destination_time,
        rt.clear_time,
        rt.canceled_time,

        -- Scheduling timestamps
        rt.appointment_time,
        rt.requested_pickup_time,
        rt.orig_pickup_time,

        -- Duration calculations (in minutes, rounded for BigQuery NUMERIC compatibility)
        ROUND(rt.call_to_assignment_minutes::numeric, 2) AS call_to_assignment_minutes,
        ROUND(rt.assignment_to_ack_minutes::numeric, 2) AS assignment_to_ack_minutes,
        ROUND(rt.ack_to_enroute_minutes::numeric, 2) AS ack_to_enroute_minutes,
        ROUND(rt.enroute_to_scene_minutes::numeric, 2) AS response_leg_minutes,
        ROUND(rt.scene_time_minutes::numeric, 2) AS scene_time_minutes,
        ROUND(rt.transport_time_minutes::numeric, 2) AS transport_time_minutes,
        ROUND(rt.destination_to_clear_minutes::numeric, 2) AS destination_to_clear_minutes,
        ROUND(rt.response_time_minutes::numeric, 2) AS response_time_minutes,
        ROUND(rt.total_unit_time_minutes::numeric, 2) AS total_unit_time_minutes,

        -- Time on Task (assigned to clear, in minutes)
        CASE
            WHEN rt.assigned_time IS NOT NULL AND rt.clear_time IS NOT NULL
            THEN ROUND((EXTRACT(EPOCH FROM (rt.clear_time - rt.assigned_time)) / 60.0)::numeric, 2)
        END AS time_on_task_minutes,

        -- Location details
        loc.pickup_facility,
        loc.pickup_city,
        loc.pickup_state,
        loc.dropoff_facility,
        loc.dropoff_city,
        loc.dropoff_state,

        -- Mileage
        r.mileage,
        CASE WHEN r.mileage > 50 THEN true ELSE false END AS is_long_distance,

        -- Crew assignment (links run to shift - join to bq_shifts in PowerBI for shift details)
        la.shift_assignment_id,

        -- Flags for filtering
        CASE
            WHEN r.calltype_name IN ('ALS', 'BLS', 'CCT') THEN true
            ELSE false
        END AS is_transport_run,

        CASE
            WHEN r.calltype_name = 'CCT' THEN true
            ELSE false
        END AS is_critical_care,

        CASE
            WHEN r.level_of_service LIKE '%Flight%' OR r.calltype_name LIKE '%Flight%' THEN true
            ELSE false
        END AS is_flight_crew,

        -- On-time calculation (pickup within 15 min of scheduled)
        CASE
            WHEN rt.pickup_time IS NOT NULL AND rt.at_scene_time IS NOT NULL
            THEN ROUND((EXTRACT(EPOCH FROM (rt.at_scene_time - rt.pickup_time)) / 60.0), 2)
        END AS pickup_variance_minutes,

        CASE
            WHEN rt.pickup_time IS NOT NULL AND rt.at_scene_time IS NOT NULL
                AND EXTRACT(EPOCH FROM (rt.at_scene_time - rt.pickup_time)) / 60.0 <= 15
            THEN true
            ELSE false
        END AS is_on_time,

        -- Time-based flags
        rt.service_date = CURRENT_DATE AS is_today,
        rt.service_date > CURRENT_DATE AS is_future,
        rt.service_date >= CURRENT_DATE AS is_today_or_future,
        rt.service_date - CURRENT_DATE AS days_from_today,

        -- Record metadata
        r.created_timestamp,
        r.modified_timestamp

    FROM runs r
    INNER JOIN run_timestamps rt
        ON r.leg_id = rt.leg_id
        AND r.source_database = rt.source_database
    LEFT JOIN run_locations loc
        ON r.leg_id = loc.leg_id
        AND r.source_database = loc.source_database
    LEFT JOIN run_cancels c
        ON r.leg_id = c.leg_id
        AND r.source_database = c.source_database
    LEFT JOIN all_leg_assignments la
        ON r.leg_id = la.leg_id
        AND r.source_database = la.source_database
    LEFT JOIN pay_periods pp
        ON rt.service_date >= pp.start_date
        AND rt.service_date <= pp.end_date
)

SELECT DISTINCT ON (leg_id, source_database)
    *
FROM runs_enriched
ORDER BY
    leg_id,
    source_database,
    shift_assignment_id NULLS LAST