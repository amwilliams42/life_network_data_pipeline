
{{ config(materialized='table') }}

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

with {% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
    {{ suffix }}_timesheet_assignments as (
        SELECT
            '{{ suffix }}' as source_database,
            stsa.id as assignment_id,
            stsa.start_time,
            stsa.end_time,
            stsa.date_line,
            users.username,
            stsa.cost_center_id,
            ts.time_id,
            TO_TIMESTAMP(ts.time_start_ts) as time_start,
            TO_TIMESTAMP(ts.time_end_ts) as time_end,
            ts.time_start_ts,
            ts.time_end_ts
        FROM {{ source(dataset, 'sched_template_shift_assignments') }} AS stsa
        LEFT JOIN {{ source(dataset, 'sched_shifts') }} AS shift
            ON shift.id = stsa.shift_id
        INNER JOIN {{ source(dataset, 'users') }}
            ON users.user_id = stsa.user_id
        LEFT JOIN {{ source(dataset, 'timesheet') }} AS ts
            ON (
                (
                    stsa.id = ts.shift_assignment_id
                    AND ts.time_user_id = stsa.user_id
                )
                OR (
                    ts.shift_assignment_id IS NULL
                    AND ABS(EXTRACT(EPOCH FROM stsa.start_time) - ts.time_start_ts::double precision) < (60 * 60)
                    AND ts.time_user_id = stsa.user_id
                    AND ts.time_start_ts::double precision < EXTRACT(EPOCH FROM stsa.end_time)
                )
            )
            AND ts.time_end_ts::bigint <> 0
        WHERE stsa.deleted = '0'
            AND stsa.published = 'true'
            AND stsa.type = 'Regular'
            AND stsa.user_id IS NOT NULL
            AND stsa.date_line >= CURRENT_DATE - INTERVAL '30 days'
    ),

    {{ suffix }}_timesheet_orphans as (
        SELECT
            '{{ suffix }}' as source_database,
            stsa.id as assignment_id,
            stsa.start_time,
            stsa.end_time,
            stsa.date_line,
            users.username,
            stsa.cost_center_id,
            ts.time_id,
            TO_TIMESTAMP(ts.time_start_ts) as time_start,
            TO_TIMESTAMP(ts.time_end_ts) as time_end,
            ts.time_start_ts,
            ts.time_end_ts
        FROM {{ source(dataset, 'timesheet') }} AS ts
        LEFT JOIN {{ source(dataset, 'sched_template_shift_assignments') }} AS stsa
            ON (
                (
                    stsa.id = ts.shift_assignment_id
                    AND ts.time_user_id = stsa.user_id
                )
                OR (
                    ts.shift_assignment_id IS NULL
                    AND ABS(EXTRACT(EPOCH FROM stsa.start_time) - ts.time_start_ts::double precision) < (60 * 60)
                    AND ts.time_user_id = stsa.user_id
                    AND ts.time_start_ts::double precision < EXTRACT(EPOCH FROM stsa.end_time)
                )
            )
        LEFT JOIN {{ source(dataset, 'sched_shifts') }} AS shift
            ON shift.id = stsa.shift_id
        INNER JOIN {{ source(dataset, 'users') }}
            ON users.user_id = ts.time_user_id
        WHERE ts.time_start_ts::bigint >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '30 days'))
    ),

    {{ suffix }}_combined as (
        SELECT * FROM {{ suffix }}_timesheet_assignments
        UNION
        SELECT * FROM {{ suffix }}_timesheet_orphans
    ){% if not loop.last %},{% endif %}
{% endfor %}

select * from tn_combined
union all
select * from mi_combined
union all
select * from il_combined
