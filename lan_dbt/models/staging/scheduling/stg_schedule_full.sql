{{ config(materialized='view') }}

/*
    Full schedule staging - no date filtering.

    Use this for BigQuery exports and historical analysis.
    For performance-sensitive models, use stg_schedule (30-day window).

    Key features:
    - Same logic as stg_schedule but without date filter
    - Joins schedule assignments with timesheet data from stg_timesheet
    - Uses effective_clock_out for hours calculation (actual or scheduled end if still clocked in)
    - Includes timesheet status flags for filtering
*/

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

WITH
stg_timesheet_data AS (
    SELECT * FROM {{ ref('stg_timesheet') }}
),

{% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}

{{ suffix }}_unit_personnel_dedup AS (
    SELECT DISTINCT ON (unit_id, slot)
        *
    FROM {{ source(dataset,'sched_unit_personnel') }}
    ORDER BY unit_id, slot, id DESC
),
{% endfor %}

{% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}

{{ suffix }}_schedule AS (
    SELECT
        -- Primary identifiers
        stsa.id AS assignment_id,
        stsa.user_id,
        '{{ suffix }}' AS source_database,

        -- Schedule timing
        stsa.date_line,
        stsa.start_time AS shift_start,
        stsa.end_time AS shift_end,
        CASE
            WHEN users.user_id IS NOT NULL THEN (EXTRACT(EPOCH FROM stsa.end_time) - EXTRACT(EPOCH FROM stsa.start_time)) / 3600
            ELSE 0
        END AS scheduled_hours,

        -- Employee information
        users.first_name,
        users.last_name,
        users.employee_num,
        users.job_title,
        CASE
            WHEN users.last_name IS NOT NULL AND users.first_name IS NOT NULL THEN
                CONCAT(users.last_name, ', ', users.first_name)
            ELSE NULL
        END AS assigned_name,

        -- Assignment status
        CASE
            WHEN users.user_id IS NOT NULL THEN 'ASSIGNED'
            ELSE 'OPEN'
        END AS assignment_status,

        -- Open hours calculation
        CASE
            WHEN users.user_id IS NOT NULL THEN 0
            ELSE (EXTRACT(EPOCH FROM stsa.end_time) - EXTRACT(EPOCH FROM stsa.start_time)) / 3600
        END AS open_hours,

        -- Unit/shift information
        stsa.unit_id,
        unit.name AS unit_name,
        stsa.slot AS position_slot,

        -- Certification/qualification requirements
        uct.template_name AS required_qualification,
        uct.min_licensure_id,
        uct.min_level_id,

        -- Cost center
        stsa.cost_center_id,
        cc.name AS cost_center_name,
        cc.shortname AS cost_center_short,

        -- Additional schedule details
        stsa.shift_id,
        stsa.published,
        stsa.status AS shift_status,
        stsa.schedule_type,
        stsa.comments,
        stsa.earning_code_id,
        ec.description AS earning_code,

        -- Pay period information from seed
        pp.pay_period_year,
        pp.pay_period_number,
        pp.start_date AS pay_period_start,
        pp.end_date AS pay_period_end,

        -- Time-based flags for reporting
        stsa.date_line = CURRENT_DATE AS is_today,
        stsa.date_line > CURRENT_DATE AS is_future,
        stsa.date_line >= CURRENT_DATE AS is_today_or_future,
        stsa.date_line - CURRENT_DATE AS days_from_today,

        -- Timesheet information
        ts.time_id,
        ts.clock_in_time,
        ts.clock_out_time,
        ts.effective_clock_out,

        -- Timesheet status flags
        COALESCE(ts.has_timesheet, FALSE) AS has_timesheet,
        COALESCE(ts.is_clocked_in, FALSE) AS is_clocked_in,
        COALESCE(ts.is_clocked_out, FALSE) AS is_clocked_out,
        ts.match_type AS timesheet_match_type,

        -- Is training?
        CASE
            WHEN uct.template_name = 'Third Party' THEN TRUE
            WHEN unit.name = 'Memphis - Orientation' THEN TRUE
            WHEN unit.name = 'Nash - Orientation' THEN TRUE
            WHEN unit.name LIKE '%Orientation%' THEN TRUE
            WHEN uct.template_name LIKE '%FTO%' THEN TRUE
            ELSE FALSE
        END AS is_training,

        -- Actual hours worked (uses effective_clock_out for still-clocked-in scenarios)
        CASE
            WHEN ts.clock_in_time IS NOT NULL AND ts.effective_clock_out IS NOT NULL THEN
                EXTRACT(EPOCH FROM (ts.effective_clock_out - ts.clock_in_time)) / 3600
            ELSE NULL
        END AS hours_worked,

        -- Pay period flags for easy filtering
        CASE
            WHEN stsa.date_line >= (SELECT start_date FROM {{ ref('pay_periods') }} WHERE start_date <= CURRENT_DATE ORDER BY start_date DESC LIMIT 1)
                 AND stsa.date_line <= (SELECT end_date FROM {{ ref('pay_periods') }} WHERE start_date <= CURRENT_DATE ORDER BY start_date DESC LIMIT 1)
            THEN TRUE ELSE FALSE
        END AS is_current_pay_period,

        CASE
            WHEN stsa.date_line >= (SELECT start_date FROM {{ ref('pay_periods') }} WHERE start_date > CURRENT_DATE ORDER BY start_date ASC LIMIT 1)
                 AND stsa.date_line <= (SELECT end_date FROM {{ ref('pay_periods') }} WHERE start_date > CURRENT_DATE ORDER BY start_date ASC LIMIT 1)
            THEN TRUE ELSE FALSE
        END AS is_next_pay_period,

        -- Day of week
        EXTRACT(dow FROM stsa.date_line) AS day_of_week,
        TO_CHAR(stsa.date_line, 'Day') AS day_name,

        -- Week and month identifiers
        DATE_TRUNC('week', stsa.date_line)::date AS week_start,
        DATE_TRUNC('month', stsa.date_line)::date AS month_start,

        -- Current timestamp for tracking
        CURRENT_TIMESTAMP AS record_created_at,

        ec.description

    FROM {{ source(dataset,'sched_template_shift_assignments') }} AS stsa
    LEFT JOIN {{ source(dataset,'users') }} AS users
        ON users.user_id = stsa.user_id
    LEFT JOIN {{ source(dataset,'sched_units') }} AS unit
        ON unit.id = stsa.unit_id
    LEFT JOIN {{ suffix }}_unit_personnel_dedup AS sup
        ON sup.unit_id = stsa.unit_id AND sup.slot = stsa.slot
    LEFT JOIN {{ source(dataset,'sched_unit_certification_templates') }} AS uct
        ON uct.id = sup.certification_template_id
    LEFT JOIN {{ source(dataset,'cost_centers') }} AS cc
        ON cc.id = stsa.cost_center_id
    LEFT JOIN {{ ref('pay_periods') }} AS pp
        ON stsa.date_line >= pp.start_date
        AND stsa.date_line <= pp.end_date
    LEFT JOIN stg_timesheet_data AS ts
        ON ts.assignment_id = stsa.id
        AND ts.source_database = '{{ suffix }}'
    LEFT JOIN {{ source(dataset, 'sched_earning_codes') }} AS ec
        ON ec.id = stsa.earning_code_id
    WHERE stsa.deleted = '0'
        -- No date filter - include all historical data
){% if not loop.last %},{% endif %}
{% endfor %}

SELECT * FROM tn_schedule
UNION ALL
SELECT * FROM mi_schedule
UNION ALL
SELECT * FROM il_schedule