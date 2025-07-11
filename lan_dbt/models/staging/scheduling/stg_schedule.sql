{{ config(materialized='view') }}

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

with {% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
    {{ suffix }}_schedule as (
    select
        stsa.id,
        uct.template_name as shift_qualification,
        CONCAT(users.last_name, ', ', users.first_name) as name,
        stsa.date_line,
        unit.name as shift_name,
        stsa.cost_center_id,
        scc.name as shift_cc,
        stsa.start_time as shift_start,
        stsa.end_time as shift_end,
        (
            EXTRACT(EPOCH FROM stsa.end_time) - EXTRACT(EPOCH FROM stsa.start_time)
        ) / 3600 as scheduled_length,
        CASE
            WHEN ts.time_end_ts = 0 OR ts.time_end_ts IS NULL THEN
                (EXTRACT(EPOCH FROM NOW()) - ts.time_start_ts)
            ELSE
                (ts.time_end_ts - ts.time_start_ts)
        END / 3600 AS worked,
        '{{ suffix }}' as source_database

    from {{ source(dataset,'sched_template_shift_assignments') }} as stsa
    left join {{ source(dataset,'sched_shifts') }} as shift on shift.id = stsa.shift_id
    left join {{ source(dataset,'users') }} as users on users.user_id = stsa.user_id
    left join {{ source(dataset,'sched_units') }} as unit on unit.id = stsa.unit_id
    left join {{ source(dataset,'sched_unit_personnel') }} as sup on sup.unit_id = stsa.unit_id and sup.slot = stsa.slot
    left join {{ source(dataset,'sched_unit_certification_templates') }} as uct on uct.id = sup.certification_template_id
    left join {{ source(dataset,'cost_centers') }} as scc on scc.id = stsa.cost_center_id
    left join {{ source(dataset,'timesheet') }} as ts on (
        (
            (
                stsa.id = ts.shift_assignment_id
                AND ts.time_user_id = users.user_id
            )
            OR (
                (
                    ts.shift_assignment_id IS NULL
                )
                AND ABS(
                    EXTRACT(EPOCH FROM stsa.start_time) - ts.time_start_ts
                ) < (60 * 60)
                AND ts.time_user_id = users.user_id
                AND ts.time_start_ts < EXTRACT(EPOCH FROM stsa.end_time)
            )
        )
    )

),

{% endfor %}

combined as (
    select * from tn_schedule
    union all
    select * from mi_schedule
    union all
    select * from il_schedule
)
select * from combined