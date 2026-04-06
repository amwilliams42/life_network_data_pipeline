{{
  config(
    severity = 'warn',
    store_failures = true,
    schema = 'dq_issues'
  )
}}

/*
  Data Quality Test: Invalid Run Timestamps

  Detects runs where the time between enroute and clear is either:
  - Negative (clear_time before enroute_time)
  - Exceeds 12 hours (720 minutes), indicating bad data

  These are logged to the dq_issues schema for review.
  Runs in warn mode so it won't fail the build.
*/

with run_timestamps as (
    select * from {{ ref('stg_run_timestamps') }}
),

runs as (
    select * from {{ ref('stg_runs') }}
),

invalid_timestamps as (
    select
        rt.leg_id,
        r.run_number,
        rt.source_database,
        rt.service_date,
        rt.enroute_time,
        rt.clear_time,
        round(extract(epoch from (rt.clear_time - rt.enroute_time)) / 60.0, 2) as calculated_time_on_task_minutes,
        case
            when rt.clear_time < rt.enroute_time then 'clear_time before enroute_time'
            when extract(epoch from (rt.clear_time - rt.enroute_time)) / 60.0 > 720 then 'time_on_task exceeds 12 hours'
        end as issue_type,
        current_timestamp as detected_at
    from run_timestamps rt
    inner join runs r
        on rt.leg_id = r.leg_id
        and rt.source_database = r.source_database
    where rt.enroute_time is not null
      and rt.clear_time is not null
      and (
          -- Clear time is before enroute time (negative duration)
          rt.clear_time < rt.enroute_time
          -- Or time on task exceeds 12 hours (720 minutes)
          or extract(epoch from (rt.clear_time - rt.enroute_time)) / 60.0 > 720
      )
)

select * from invalid_timestamps