{{ config(materialized='table') }}

/*
    BigQuery Shifts Export

    Comprehensive shift-level detail for PowerBI reporting.
    No date filtering - includes all available historical data.

    Join to bq_users via user_id + source_database for employee details.

    Key metrics calculable in DAX:
    - Units up by hour (using shift_start/shift_end overlap)
    - Scheduled vs actual hours
    - Open shift analysis
    - Overtime tracking
*/

WITH
schedule_raw AS (
    SELECT * FROM {{ ref('stg_schedule_full') }}
),

-- Deduplicate shifts: source database sometimes creates duplicate records
-- with null clock in/out times. Prefer records with actual clock times.
schedule AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY assignment_id, source_database
                ORDER BY clock_in_time NULLS LAST
            ) AS _dedup_rank
        FROM schedule_raw
    )
    WHERE _dedup_rank = 1
),

-- Get shift partners (other crew members on the same unit/shift)
shift_partners AS (
    SELECT
        s1.assignment_id,
        s1.source_database,
        STRING_AGG(
            DISTINCT s2.assigned_name,
            ', '
            ORDER BY s2.assigned_name
        ) FILTER (WHERE s2.assignment_id != s1.assignment_id) AS partner_names,
        COUNT(DISTINCT s2.assignment_id) FILTER (WHERE s2.assignment_id != s1.assignment_id) AS partner_count
    FROM schedule s1
    LEFT JOIN schedule s2
        ON s1.unit_id = s2.unit_id
        AND s1.date_line = s2.date_line
        AND s1.shift_start = s2.shift_start
        AND s1.source_database = s2.source_database
    GROUP BY s1.assignment_id, s1.source_database
),

-- Shift staffing summary (positions filled vs open per shift)
shift_staffing AS (
    SELECT
        source_database,
        unit_id,
        date_line,
        shift_start,
        COUNT(*) AS total_positions,
        COUNT(*) FILTER (WHERE assignment_status = 'ASSIGNED') AS filled_positions,
        COUNT(*) FILTER (WHERE assignment_status = 'OPEN') AS open_positions
    FROM schedule
    GROUP BY source_database, unit_id, date_line, shift_start
)

SELECT
    -- Primary identifiers
    s.assignment_id,
    s.shift_id,
    s.user_id,
    s.source_database,
    s.assigned_name,

    -- Region: split TN into Memphis and Nashville
    CASE
        WHEN s.source_database = 'il' THEN 'il'
        WHEN s.source_database = 'mi' THEN 'mi'
        WHEN s.source_database = 'tn' AND (
            s.cost_center_name LIKE 'Memp%'
            OR s.cost_center_name LIKE 'Miss%'
        ) THEN 'tn_memphis'
        WHEN s.source_database = 'tn' AND s.cost_center_name LIKE 'Nash%' THEN 'tn_nashville'
        ELSE s.source_database
    END AS region,

    -- Level of service derived from cost center
    CASE
        WHEN s.cost_center_name LIKE '%BLS%' OR s.cost_center_name LIKE '%EMT%' THEN 'BLS'
        WHEN s.cost_center_name LIKE '%ALS%' OR s.cost_center_name LIKE '%Paramedic%' THEN 'ALS'
        WHEN s.cost_center_name LIKE '%Critical Care%' OR s.cost_center_name LIKE '%CCT%' THEN 'CCT'
        WHEN s.cost_center_name LIKE '%LDT%' THEN 'LDT'
        WHEN s.cost_center_name LIKE '%Special Event%' THEN 'Special Event'
        WHEN s.cost_center_name LIKE '%Orientation%' THEN 'Orientation'
        ELSE 'Other'
    END AS level_of_service,

    -- Date and time
    s.date_line AS shift_date,
    s.day_of_week,
    s.day_name,
    s.week_start,
    s.month_start,

    -- Scheduled shift times
    s.shift_start,
    s.shift_end,
    ROUND(s.scheduled_hours::numeric, 2) AS scheduled_hours,

    -- Actual clock in/out times
    s.clock_in_time,
    s.clock_out_time,
    ROUND(s.hours_difference::numeric, 2) AS actual_hours_worked,

    -- Variance
    CASE
        WHEN s.hours_difference IS NOT NULL
        THEN ROUND((s.hours_difference - s.scheduled_hours)::numeric, 2)
        ELSE NULL
    END AS hours_variance,

    -- Unit information
    s.unit_id,
    s.unit_name,
    s.position_slot,
    s.required_qualification,

    -- Cost center
    s.cost_center_id,
    s.cost_center_name,

    -- Assignment status
    s.assignment_status,
    CASE WHEN s.assignment_status = 'OPEN' THEN true ELSE false END AS is_open,
    CASE WHEN s.assignment_status = 'ASSIGNED' THEN true ELSE false END AS is_assigned,

    -- Shift type flags
    s.is_training,
    CASE
        WHEN s.cost_center_name LIKE '%Special Event%' THEN true
        ELSE false
    END AS is_special_event,
    CASE
        WHEN s.cost_center_name LIKE '%Orientation%'
            OR s.cost_center_name LIKE '%FTO%'
            OR s.required_qualification LIKE '%FTO%'
            OR s.required_qualification = 'Third Party'
        THEN true
        ELSE false
    END AS is_orientation,

    -- Field shift: actual operational shifts (excludes special events and orientation)
    CASE
        WHEN (s.cost_center_id, s.source_database) IN (
            (6, 'il'),   -- EMT Carol Stream
            (10, 'il'),  -- EMT Chicago
            (3, 'il'),   -- EMT Skokie
            (8, 'il'),   -- Paramedic Chicago
            (4, 'il'),   -- Paramedic Skokie
            (22, 'mi'),  -- EMT Oakland County
            (20, 'mi'),  -- EMT Wayne County
            (11, 'mi'),  -- Lincoln Park Rescue
            (23, 'mi'),  -- Paramedic Oakland County
            (21, 'mi'),  -- Paramedic Wayne County
            (70, 'tn'),  -- Memp - Critical Care
            (47, 'tn'),  -- Memp - EMT BLS
            (63, 'tn'),  -- Memp - LDT
            (52, 'tn'),  -- Memp - Paramedic ALS
            (48, 'tn'),  -- Miss - EMT BLS
            (53, 'tn'),  -- Miss - Paramedic ALS
            (49, 'tn'),  -- Nash - EMT BLS
            (64, 'tn'),  -- Nash - LDT
            (54, 'tn')   -- Nash - Paramedic ALS
        ) THEN true
        ELSE false
    END AS is_field_shift,

    -- Attendance flags
    CASE
        WHEN s.assignment_status = 'ASSIGNED' AND s.clock_in_time IS NULL
            AND s.date_line < CURRENT_DATE
        THEN true
        ELSE false
    END AS is_no_show,

    CASE
        WHEN s.assignment_status = 'ASSIGNED'
            AND s.hours_difference IS NOT NULL
            AND s.hours_difference < s.scheduled_hours * 0.9
        THEN true
        ELSE false
    END AS is_partial_shift,

    -- Shift staffing context
    ss.total_positions,
    ss.filled_positions,
    ss.open_positions,
    CASE
        WHEN ss.open_positions > 0 THEN true
        ELSE false
    END AS shift_has_openings,

    -- Partner information
    sp.partner_names,
    sp.partner_count,

    -- Pay period information
    s.pay_period_year,
    s.pay_period_number,
    s.pay_period_start,
    s.pay_period_end,
    s.is_current_pay_period,
    s.is_next_pay_period,

    -- Earning code
    s.earning_code_id,
    s.description AS earning_code_description,

    -- Time-based flags for filtering
    s.is_today,
    s.is_future,
    s.is_today_or_future,
    s.days_from_today

FROM schedule s
LEFT JOIN shift_partners sp
    ON s.assignment_id = sp.assignment_id
    AND s.source_database = sp.source_database
LEFT JOIN shift_staffing ss
    ON s.source_database = ss.source_database
    AND s.unit_id = ss.unit_id
    AND s.date_line = ss.date_line
    AND s.shift_start = ss.shift_start
ORDER BY
    s.source_database,
    s.date_line,
    s.shift_start,
    s.unit_name