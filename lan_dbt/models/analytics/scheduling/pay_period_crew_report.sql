{{ config(materialized='view') }}

/*
    Pay Period Crew Report

    Field shifts only (crews), grouped by pay period from source-specific pay periods.
    Filters out special events and orientation. Excludes open training shifts.

    Use with source_database and pay_period_start/end filters for specific reports.
*/

WITH
pay_periods AS (
    SELECT * FROM {{ ref('stg_pay_periods') }}
),

shifts AS (
    SELECT
        s.*,
        pp.pay_period_id,
        pp.pay_period_number AS pp_number,
        pp.start_date AS pp_start,
        pp.end_date AS pp_end,
        -- Week 1 or 2 within pay period
        CASE
            WHEN s.shift_date < pp.start_date + 7 THEN 1
            ELSE 2
        END AS pp_week
    FROM {{ ref('bq_shifts') }} s
    INNER JOIN pay_periods pp
        ON s.source_database = pp.source_database
        AND s.shift_date >= pp.start_date
        AND s.shift_date <= pp.end_date
    WHERE s.is_field_shift = true
      AND s.is_special_event = false
      AND s.is_orientation = false
      -- Exclude open training shifts
      AND NOT (s.is_open = true AND s.is_training = true)
      -- Must have a unit
      AND s.unit_name IS NOT NULL
),

-- Employee weekly hours (for overtime calculation)
employee_weekly AS (
    SELECT
        source_database,
        user_id,
        pp_number,
        pp_week,
        SUM(scheduled_hours) AS week_scheduled_hours,
        SUM(actual_hours_worked) AS week_actual_hours,
        COUNT(*) AS week_shift_count
    FROM shifts
    WHERE is_assigned = true
      AND user_id IS NOT NULL
    GROUP BY source_database, user_id, pp_number, pp_week
),

-- Weekly overtime
employee_weekly_ot AS (
    SELECT
        *,
        CASE WHEN week_scheduled_hours > 40 THEN week_scheduled_hours - 40 ELSE 0 END AS week_scheduled_ot,
        CASE WHEN week_actual_hours > 40 THEN week_actual_hours - 40 ELSE 0 END AS week_actual_ot
    FROM employee_weekly
),

-- Pay period totals
employee_pp_totals AS (
    SELECT
        source_database,
        user_id,
        pp_number,
        SUM(week_scheduled_hours) AS pp_scheduled_hours,
        SUM(week_actual_hours) AS pp_actual_hours,
        SUM(week_scheduled_ot) AS pp_scheduled_ot,
        SUM(week_actual_ot) AS pp_actual_ot,
        SUM(week_shift_count) AS pp_shift_count
    FROM employee_weekly_ot
    GROUP BY source_database, user_id, pp_number
)

SELECT
    -- Pay period info
    s.pp_number AS pay_period_number,
    s.pp_start AS pay_period_start,
    s.pp_end AS pay_period_end,
    s.pp_week AS pay_period_week,

    -- Identifiers
    s.source_database,
    s.region,
    s.assignment_id,
    s.user_id,

    -- Shift date/time
    s.shift_date,
    s.day_name,
    s.shift_start,
    s.shift_end,
    s.scheduled_hours,
    s.actual_hours_worked,

    -- Employee info
    s.assigned_name,
    s.earning_code,
    s.level_of_service,

    -- Unit info
    s.unit_id,
    s.unit_name,
    s.cost_center_name,

    -- Status flags
    s.is_open,
    s.is_assigned,
    s.is_training,
    s.assignment_status,

    -- Staffing
    s.total_positions,
    s.filled_positions,
    s.open_positions,
    s.shift_has_openings,
    s.partner_names,
    s.partner_count,

    -- Employee weekly totals (for this week)
    COALESCE(ew.week_scheduled_hours, 0) AS employee_week_scheduled,
    COALESCE(ew.week_actual_hours, 0) AS employee_week_actual,
    COALESCE(ew.week_scheduled_ot, 0) AS employee_week_ot,

    -- Employee pay period totals
    COALESCE(ep.pp_scheduled_hours, 0) AS employee_pp_scheduled,
    COALESCE(ep.pp_actual_hours, 0) AS employee_pp_actual,
    COALESCE(ep.pp_scheduled_ot, 0) AS employee_pp_scheduled_ot,
    COALESCE(ep.pp_actual_ot, 0) AS employee_pp_actual_ot,

    -- Cumulative hours through the week
    SUM(s.scheduled_hours) OVER (
        PARTITION BY s.source_database, s.user_id, s.pp_number, s.pp_week
        ORDER BY s.shift_date, s.shift_start
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_week_hours

FROM shifts s
LEFT JOIN employee_weekly_ot ew
    ON s.source_database = ew.source_database
    AND s.user_id = ew.user_id
    AND s.pp_number = ew.pp_number
    AND s.pp_week = ew.pp_week
LEFT JOIN employee_pp_totals ep
    ON s.source_database = ep.source_database
    AND s.user_id = ep.user_id
    AND s.pp_number = ep.pp_number

ORDER BY s.region, s.pp_week, s.shift_date, s.shift_start, s.unit_name
