{{ config(materialized='view') }}

select
  r.*,
  loc.pickup_facility,
  case
    when r.last_status_id > 0 then 'ran'
    when r.last_status_id < 0 and c.lost_call_status is not null then 'turned'
    when r.last_status_id < 0 and c.lost_call_status is null then 'cancelled'
    when r.last_status_id is null then 'cancelled'
    else 'unknown'
  end as run_outcome,
  -- Full day name without trailing spaces
  to_char(t.pickup_time, 'FMDay') as day_of_week,
  -- Military hour bucket (e.g., 0000, 0100, ..., 2300)
  to_char(t.pickup_time, 'HH24') || '00' as hour_of_day
from {{ ref('stg_runs') }} r
left join {{ ref('stg_run_locations') }} loc
  on loc.leg_id = r.leg_id
 and loc.source_database = r.source_database
left join {{ ref('stg_run_cancels') }} c
  on c.leg_id = r.leg_id
 and c.source_database = r.source_database
left join {{ ref('stg_run_timestamps') }} t
  on t.leg_id = r.leg_id
 and t.source_database = r.source_database
where r.source_database = 'tn' and
      r.market = 'Nashville'