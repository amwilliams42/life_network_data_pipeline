{{ config(materialized='table') }}

with runs as (
    select * from {{ ref('stg_runs') }}
),

timestamps as (
    select * from {{ ref('stg_run_timestamps') }}
),

locations as (
    select * from {{ ref('stg_run_locations') }}
),

cancels as (
    select * from {{ ref('stg_run_cancels') }}
),

-- Combine all run data with outcome classification
run_details as (
    select
        r.leg_id,
        r.run_number,
        r.date_of_service,
        r.level_of_service,
        r.source_database,
        r.market,
        r.last_status_id,
        r.created_timestamp,

        -- Region: split TN into Memphis and Nashville
        case
            when r.source_database = 'il' then 'il'
            when r.source_database = 'mi' then 'mi'
            when r.source_database = 'tn' and (r.market = 'Memphis' or r.market = 'Mississippi') then 'tn_memphis'
            when r.source_database = 'tn' and r.market = 'Nashville' then 'tn_nashville'
            else r.source_database
        end as region,

        -- Timestamps for time metrics
        t.scene_time_minutes,
        t.destination_to_clear_minutes,

        -- Location/mileage
        loc.mileage,

        -- Cancel info for outcome classification
        c.lost_call_status,

        -- Classify run outcome
        case
            when r.last_status_id > 0 then 'ran'
            when r.last_status_id < 0 and c.lost_call_status is not null then 'turned'
            when r.last_status_id < 0 and c.lost_call_status is null then 'cancelled'
            when r.last_status_id is null then 'cancelled'
            else 'unknown'
        end as run_outcome,

        -- Pre-scheduled: created before 3pm the day before service
        case
            when r.created_timestamp < (r.date_of_service - interval '1 day' + interval '15 hours')
            then true
            else false
        end as is_prescheduled

    from runs r
    left join timestamps t
        on t.leg_id = r.leg_id
        and t.source_database = r.source_database
    left join locations loc
        on loc.leg_id = r.leg_id
        and loc.source_database = r.source_database
    left join cancels c
        on c.leg_id = r.leg_id
        and c.source_database = r.source_database
),

-- Aggregate by date and region
daily_metrics as (
    select
        date_of_service,
        region,

        -- Total transports (ran only)
        count(case when run_outcome = 'ran' then leg_id end) as total_transports,

        -- Transports by level of service
        count(case when run_outcome = 'ran' and level_of_service ilike '%ALS%' then leg_id end) as transports_als,
        count(case when run_outcome = 'ran' and level_of_service ilike '%BLS%' then leg_id end) as transports_bls,
        count(case when run_outcome = 'ran' and (level_of_service ilike '%SCT%' or level_of_service ilike '%CCT%') then leg_id end) as transports_sct_cct,

        -- Pre-scheduled runs (all outcomes)
        count(case when is_prescheduled then leg_id end) as prescheduled_runs,

        -- Long distance runs (> 50 miles, ran only)
        count(case when run_outcome = 'ran' and mileage > 50 then leg_id end) as runs_over_50_miles,

        -- Long scene times (> 45 minutes, ran only)
        count(case when run_outcome = 'ran' and scene_time_minutes > 45 then leg_id end) as scene_time_over_45_min,

        -- Long destination times (> 45 minutes, ran only)
        count(case when run_outcome = 'ran' and destination_to_clear_minutes > 45 then leg_id end) as destination_time_over_45_min,

        -- Turns
        count(case when run_outcome = 'turned' then leg_id end) as turns,

        -- Cancels
        count(case when run_outcome = 'cancelled' then leg_id end) as cancels,

        -- Total calls (all outcomes)
        count(leg_id) as total_calls

    from run_details
    where date_of_service is not null
    group by date_of_service, region
)

select
    date_of_service,
    region,
    total_transports,
    transports_als,
    transports_bls,
    transports_sct_cct,
    prescheduled_runs,
    runs_over_50_miles,
    scene_time_over_45_min,
    destination_time_over_45_min,
    turns,
    cancels,
    total_calls,

    -- Calculated percentages
    case when total_calls > 0
        then round(100.0 * prescheduled_runs / total_calls, 1)
        else 0
    end as prescheduled_pct,

    case when total_transports > 0
        then round(100.0 * runs_over_50_miles / total_transports, 1)
        else 0
    end as long_distance_pct,

    case when total_calls > 0
        then round(100.0 * turns / total_calls, 1)
        else 0
    end as turn_rate_pct,

    case when total_calls > 0
        then round(100.0 * cancels / total_calls, 1)
        else 0
    end as cancel_rate_pct

from daily_metrics
order by date_of_service desc, region