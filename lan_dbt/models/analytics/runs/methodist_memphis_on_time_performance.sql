{{ config(materialized='view') }}

with methodist_runs as (
    SELECT
        stg.run_number,
        stg.leg_id,
        stg.pcr_number,
        stg.date_of_service,
        stg.source_name,
        stg.emergency,
        ts.requested_time,
        ts.pickup_time,
        ts.at_scene_time,

        -- Calculate Methodist-specific response time
        -- For emergent calls: use requested_time (or pickup_time) - at_scene_time
        -- For non-emergent: use (requested_time or pickup_time + 15 minutes) - at_scene_time
        CASE
            WHEN ts.at_scene_time IS NULL THEN NULL
            WHEN stg.emergency = 1 THEN
                -- Emergent: requested_time or pickup_time to at_scene_time
                CASE
                    WHEN ts.requested_time IS NOT NULL THEN EXTRACT(EPOCH FROM (ts.at_scene_time - ts.requested_time))/60.0
                    WHEN ts.pickup_time IS NOT NULL THEN EXTRACT(EPOCH FROM (ts.at_scene_time - ts.pickup_time))/60.0
                    ELSE NULL
                END
            ELSE
                -- Non-emergent: (requested_time or pickup_time + 15) to at_scene_time
                CASE
                    WHEN ts.requested_time IS NOT NULL THEN EXTRACT(EPOCH FROM (ts.at_scene_time - ts.requested_time))/60.0
                    WHEN ts.pickup_time IS NOT NULL THEN EXTRACT(EPOCH FROM (ts.at_scene_time - (ts.pickup_time + INTERVAL '15 minutes')))/60.0
                    ELSE NULL
                END
        END as methodist_response_time_minutes,

        -- Determine SLA threshold based on emergency status
        CASE
            WHEN stg.emergency = 1 THEN 30
            ELSE 60
        END as sla_threshold_minutes,

        -- Calculate on-time status
        CASE
            WHEN ts.at_scene_time IS NULL THEN NULL
            WHEN stg.emergency = 1 THEN
                CASE
                    WHEN ts.requested_time IS NOT NULL THEN EXTRACT(EPOCH FROM (ts.at_scene_time - ts.requested_time))/60.0 <= 30
                    WHEN ts.pickup_time IS NOT NULL THEN EXTRACT(EPOCH FROM (ts.at_scene_time - ts.pickup_time))/60.0 <= 30
                    ELSE NULL
                END
            ELSE
                CASE
                    WHEN ts.requested_time IS NOT NULL THEN EXTRACT(EPOCH FROM (ts.at_scene_time - ts.requested_time))/60.0 <= 60
                    WHEN ts.pickup_time IS NOT NULL THEN EXTRACT(EPOCH FROM (ts.at_scene_time - (ts.pickup_time + INTERVAL '15 minutes')))/60.0 <= 60
                    ELSE NULL
                END
        END as is_on_time,

        -- Calculate how early/late the response was
        CASE
            WHEN ts.at_scene_time IS NULL THEN NULL
            WHEN stg.emergency = 1 THEN
                CASE
                    WHEN ts.requested_time IS NOT NULL THEN EXTRACT(EPOCH FROM (ts.at_scene_time - ts.requested_time))/60.0 - 30
                    WHEN ts.pickup_time IS NOT NULL THEN EXTRACT(EPOCH FROM (ts.at_scene_time - ts.pickup_time))/60.0 - 30
                    ELSE NULL
                END
            ELSE
                CASE
                    WHEN ts.requested_time IS NOT NULL THEN EXTRACT(EPOCH FROM (ts.at_scene_time - ts.requested_time))/60.0 - 60
                    WHEN ts.pickup_time IS NOT NULL THEN EXTRACT(EPOCH FROM (ts.at_scene_time - (ts.pickup_time + INTERVAL '15 minutes')))/60.0 - 60
                    ELSE NULL
                END
        END as minutes_from_sla

    FROM {{ ref('stg_runs') }} as stg
    INNER JOIN {{ ref('stg_run_timestamps') }} as ts
        ON ts.leg_id = stg.leg_id
        AND ts.source_database = stg.source_database
    WHERE
        stg.source_database = 'tn'
        AND stg.source_name LIKE '%IOC%'
        and stg.run_number is not null
        and stg.trip_status > 0
)

SELECT * FROM methodist_runs
ORDER BY date_of_service DESC, pickup_time DESC