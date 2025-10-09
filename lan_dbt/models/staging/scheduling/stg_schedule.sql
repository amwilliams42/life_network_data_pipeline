{{ config(materialized='view') }}

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

with
    stg_timesheet_data as (
        select distinct on (assignment_id, source_database)
            *
        from {{ ref('stg_timesheet') }}
        order by assignment_id, source_database, time_start desc
    ),

    {% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
    {{ suffix }}_unit_personnel_dedup as (
        select distinct on (unit_id, slot)
            *
        from {{ source(dataset,'sched_unit_personnel') }}
        order by unit_id, slot, id desc
    ),
{% endfor %}

    {% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
    {{ suffix }}_schedule as (
        select
            -- Primary identifiers
            stsa.id as assignment_id,
            stsa.user_id,
            '{{ suffix }}' as source_database,

            -- Schedule timing
            stsa.date_line,
            stsa.start_time as shift_start,
            stsa.end_time as shift_end,
            CASE
                WHEN users.user_id IS NOT NULL THEN (EXTRACT(EPOCH FROM stsa.end_time) - EXTRACT(EPOCH FROM stsa.start_time)) / 3600
                ELSE 0
            END as scheduled_hours,

            -- Employee information
            users.first_name,
            users.last_name,
            users.employee_num,
            users.job_title,
                CASE
                    WHEN users.last_name IS NOT NULL AND users.first_name IS NOT NULL THEN
                        CONCAT(users.last_name, ', ', users.first_name)
                    ELSE NULL
                END as assigned_name,

            -- Assignment status
            CASE
                WHEN users.user_id IS NOT NULL THEN 'ASSIGNED'
                ELSE 'OPEN'
            END as assignment_status,

            -- Open hours calculation
            CASE
                WHEN users.user_id IS NOT NULL THEN 0
                ELSE (EXTRACT(EPOCH FROM stsa.end_time) - EXTRACT(EPOCH FROM stsa.start_time)) / 3600
            END as open_hours,

            -- Unit/shift information
            stsa.unit_id,
            unit.name as unit_name,
            stsa.slot as position_slot,

            -- Certification/qualification requirements
            uct.template_name as required_qualification,
            uct.min_licensure_id,
            uct.min_level_id,

            -- Cost center
            stsa.cost_center_id,
            cc.name as cost_center_name,
            cc.shortname as cost_center_short,
            
            -- Additional schedule details
            stsa.shift_id,
            stsa.published,
            stsa.status as shift_status,
            stsa.schedule_type,
            stsa.comments,
            stsa.earning_code_id,
            
            -- Pay period information from seed
            pp.pay_period_year,
            pp.pay_period_number,
            pp.start_date as pay_period_start,
            pp.end_date as pay_period_end,
            
            -- Time-based flags for reporting
            stsa.date_line = CURRENT_DATE as is_today,
            stsa.date_line > CURRENT_DATE as is_future,
            stsa.date_line >= CURRENT_DATE as is_today_or_future,
            stsa.date_line - CURRENT_DATE as days_from_today,

            -- Timesheet information
            ts.time_start as clock_in_time,
            ts.time_end as clock_out_time,

            -- Is training?

            CASE
                WHEN uct.template_name = 'Third Party' THEN true
                WHEN unit.name = 'Memphis - Orientation' THEN true
                WHEN unit.name = 'Nash - Orientation' THEN true
                WHEN unit.name like '%Orientation%' THEN true
                WHEN uct.template_name like '%FTO%' THEN true
                ELSE false
            END as is_training,

            -- actual time worked in hours
            case
                when  EXTRACT(EPOCH FROM (ts.time_end - ts.time_start))/3600 < 0 then (EXTRACT(EPOCH FROM stsa.end_time) - EXTRACT(EPOCH FROM stsa.start_time)) / 3600
                else EXTRACT(EPOCH FROM (ts.time_end - ts.time_start))/3600
            end as hours_difference,
            
            -- Pay period flags for easy filtering
            CASE 
                WHEN stsa.date_line >= (SELECT start_date FROM {{ ref('pay_periods') }} WHERE start_date <= CURRENT_DATE ORDER BY start_date DESC LIMIT 1)
                     AND stsa.date_line <= (SELECT end_date FROM {{ ref('pay_periods') }} WHERE start_date <= CURRENT_DATE ORDER BY start_date DESC LIMIT 1)
                THEN true ELSE false
            END as is_current_pay_period,
            
            CASE 
                WHEN stsa.date_line >= (SELECT start_date FROM {{ ref('pay_periods') }} WHERE start_date > CURRENT_DATE ORDER BY start_date ASC LIMIT 1)
                     AND stsa.date_line <= (SELECT end_date FROM {{ ref('pay_periods') }} WHERE start_date > CURRENT_DATE ORDER BY start_date ASC LIMIT 1)
                THEN true ELSE false
            END as is_next_pay_period,
            
            -- Day of week
            EXTRACT(dow FROM stsa.date_line) as day_of_week,
            TO_CHAR(stsa.date_line, 'Day') as day_name,
            
            -- Week and month identifiers
            DATE_TRUNC('week', stsa.date_line)::date as week_start,
            DATE_TRUNC('month', stsa.date_line)::date as month_start,
            
            -- Current timestamp for tracking
            CURRENT_TIMESTAMP as record_created_at,

            ec.description

        from {{ source(dataset,'sched_template_shift_assignments') }} as stsa
        left join {{ source(dataset,'users') }} as users
            on users.user_id = stsa.user_id
        left join {{ source(dataset,'sched_units') }} as unit
            on unit.id = stsa.unit_id
        left join {{ suffix }}_unit_personnel_dedup as sup
            on sup.unit_id = stsa.unit_id and sup.slot = stsa.slot
        left join {{ source(dataset,'sched_unit_certification_templates') }} as uct 
            on uct.id = sup.certification_template_id
        left join {{ source(dataset,'cost_centers') }} as cc 
            on cc.id = stsa.cost_center_id
        left join {{ ref('pay_periods') }} as pp
            on stsa.date_line >= pp.start_date
            and stsa.date_line <= pp.end_date
        left join stg_timesheet_data as ts
            on ts.assignment_id = stsa.id
            and ts.source_database = '{{ suffix }}'
        left join {{ source(dataset, 'sched_earning_codes') }} as ec
                on ec.id = stsa.earning_code_id
        where stsa.deleted = '0' 
          and (stsa.earning_code_id = 1 or ec.description like '%Regular%')
          and stsa.date_line >= CURRENT_DATE - INTERVAL '30 days'  -- Keep recent history and future
    ){% if not loop.last %},{% endif %}
{% endfor %}

select * from tn_schedule
union all
select * from mi_schedule
union all
select * from il_schedule