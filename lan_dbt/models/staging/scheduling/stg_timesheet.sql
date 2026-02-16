{{ config(materialized='table') }}

/*
    Staging model for timesheets joined to schedule assignments.

    This model captures:
    1. Scheduled shifts with matching timesheet punches (direct or fuzzy match)
    2. Orphan timesheet punches that don't match any scheduled shift
    3. Multiple timesheet entries per shift (e.g., partial shifts, cost center switches)

    Matching logic:
    - Direct match: timesheet.shift_assignment_id = assignment.id AND same user
    - Fuzzy match: Same user + timesheet falls within shift window (within 1 hour of start, or during shift)
      This catches cases where shift_assignment_id is NULL, wrong, or points to a different user's assignment

    Timezone conversion:
    - IL and TN: America/Chicago (Central Time)
    - MI: America/Detroit (Eastern Time)

    No date filtering - contains all historical data.
    Downstream models (stg_schedule, stg_schedule_full) apply date filters as needed.
*/

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

WITH
{% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
{% if suffix == 'mi' %}
{% set local_tz = 'America/Detroit' %}
{% else %}
{% set local_tz = 'America/Chicago' %}
{% endif %}

-- Assignments with matching timesheets (direct or fuzzy match)
-- Note: An assignment can have MULTIPLE timesheet entries (partial shifts, breaks, etc.)
{{ suffix }}_assignment_timesheets AS (
    SELECT
        '{{ suffix }}' AS source_database,
        stsa.id AS assignment_id,
        stsa.user_id,
        users.username,

        -- Scheduled times (already in local time from source)
        stsa.start_time AS scheduled_start,
        stsa.end_time AS scheduled_end,
        stsa.date_line,

        -- Cost center for downstream filtering
        stsa.cost_center_id,

        -- Timesheet data with timezone conversion (UTC -> local)
        ts.time_id,
        ts.time_user_id,
        (TO_TIMESTAMP(ts.time_start_ts) AT TIME ZONE 'UTC') AT TIME ZONE '{{ local_tz }}' AS clock_in_time,
        CASE
            WHEN ts.time_end_ts::bigint = 0 THEN NULL
            ELSE (TO_TIMESTAMP(ts.time_end_ts) AT TIME ZONE 'UTC') AT TIME ZONE '{{ local_tz }}'
        END AS clock_out_time,
        ts.time_start_ts,
        ts.time_end_ts,

        -- Effective clock out: actual if clocked out, scheduled end if still clocked in
        CASE
            WHEN ts.time_end_ts::bigint != 0 THEN (TO_TIMESTAMP(ts.time_end_ts) AT TIME ZONE 'UTC') AT TIME ZONE '{{ local_tz }}'
            WHEN ts.time_id IS NOT NULL THEN stsa.end_time  -- Still clocked in, use scheduled end
            ELSE NULL  -- No timesheet record
        END AS effective_clock_out,

        -- Status flags
        ts.time_id IS NOT NULL AS has_timesheet,
        ts.time_end_ts::bigint = 0 AND ts.time_id IS NOT NULL AS is_clocked_in,
        ts.time_end_ts::bigint != 0 AND ts.time_id IS NOT NULL AS is_clocked_out,
        TRUE AS has_assignment,

        -- Match type for debugging/analysis
        CASE
            WHEN ts.time_id IS NULL THEN 'no_timesheet'
            WHEN stsa.id = ts.shift_assignment_id THEN 'direct_match'
            ELSE 'fuzzy_match'
        END AS match_type

    FROM {{ source(dataset, 'sched_template_shift_assignments') }} AS stsa
    INNER JOIN {{ source(dataset, 'users') }} AS users
        ON users.user_id = stsa.user_id
    LEFT JOIN {{ source(dataset, 'timesheet') }} AS ts
        ON ts.time_user_id = stsa.user_id
        AND (
            -- Direct match: shift_assignment_id points to this assignment
            stsa.id = ts.shift_assignment_id
            OR
            -- Fuzzy match: timesheet falls within shift window
            -- This catches wrong/null shift_assignment_id, or timesheets linked to open/other-user assignments
            (
                -- Within 1 hour of shift start
                ABS(EXTRACT(EPOCH FROM stsa.start_time) - ts.time_start_ts::double precision) < 3600
                OR
                -- Timesheet starts during the shift window
                (
                    ts.time_start_ts::double precision >= EXTRACT(EPOCH FROM stsa.start_time)
                    AND ts.time_start_ts::double precision < EXTRACT(EPOCH FROM stsa.end_time)
                )
            )
        )
    WHERE stsa.deleted = '0'
        AND stsa.published = 'true'
        AND stsa.type = 'Regular'
        AND stsa.user_id IS NOT NULL
),

-- Orphan timesheets: punches that don't match any assignment for that user
{{ suffix }}_orphan_timesheets AS (
    SELECT
        '{{ suffix }}' AS source_database,
        NULL::bigint AS assignment_id,
        ts.time_user_id AS user_id,
        users.username,

        -- No scheduled times for orphans
        NULL::timestamp AS scheduled_start,
        NULL::timestamp AS scheduled_end,
        ((TO_TIMESTAMP(ts.time_start_ts) AT TIME ZONE 'UTC') AT TIME ZONE '{{ local_tz }}')::date AS date_line,

        -- No cost center for orphans
        NULL::bigint AS cost_center_id,

        -- Timesheet data with timezone conversion
        ts.time_id,
        ts.time_user_id,
        (TO_TIMESTAMP(ts.time_start_ts) AT TIME ZONE 'UTC') AT TIME ZONE '{{ local_tz }}' AS clock_in_time,
        CASE
            WHEN ts.time_end_ts::bigint = 0 THEN NULL
            ELSE (TO_TIMESTAMP(ts.time_end_ts) AT TIME ZONE 'UTC') AT TIME ZONE '{{ local_tz }}'
        END AS clock_out_time,
        ts.time_start_ts,
        ts.time_end_ts,

        -- Effective clock out: for orphans still clocked in, use NULL (no scheduled end to fall back on)
        CASE
            WHEN ts.time_end_ts::bigint != 0 THEN (TO_TIMESTAMP(ts.time_end_ts) AT TIME ZONE 'UTC') AT TIME ZONE '{{ local_tz }}'
            ELSE NULL  -- Still clocked in with no schedule to reference
        END AS effective_clock_out,

        -- Status flags
        TRUE AS has_timesheet,
        ts.time_end_ts::bigint = 0 AS is_clocked_in,
        ts.time_end_ts::bigint != 0 AS is_clocked_out,
        FALSE AS has_assignment,
        'orphan' AS match_type

    FROM {{ source(dataset, 'timesheet') }} AS ts
    INNER JOIN {{ source(dataset, 'users') }} AS users
        ON users.user_id = ts.time_user_id
    WHERE NOT EXISTS (
        -- Exclude timesheets that match any assignment for this user (by time window)
        SELECT 1
        FROM {{ source(dataset, 'sched_template_shift_assignments') }} AS stsa
        WHERE stsa.deleted = '0'
            AND stsa.published = 'true'
            AND stsa.type = 'Regular'
            AND stsa.user_id = ts.time_user_id
            AND (
                -- Direct match
                stsa.id = ts.shift_assignment_id
                OR
                -- Fuzzy match by time window
                ABS(EXTRACT(EPOCH FROM stsa.start_time) - ts.time_start_ts::double precision) < 3600
                OR
                (
                    ts.time_start_ts::double precision >= EXTRACT(EPOCH FROM stsa.start_time)
                    AND ts.time_start_ts::double precision < EXTRACT(EPOCH FROM stsa.end_time)
                )
            )
    )
),

{{ suffix }}_combined AS (
    SELECT * FROM {{ suffix }}_assignment_timesheets
    UNION ALL
    SELECT * FROM {{ suffix }}_orphan_timesheets
){% if not loop.last %},{% endif %}
{% endfor %}

SELECT * FROM tn_combined
UNION ALL
SELECT * FROM mi_combined
UNION ALL
SELECT * FROM il_combined