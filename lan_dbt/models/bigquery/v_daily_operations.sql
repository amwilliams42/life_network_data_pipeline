{{ config(materialized='table') }}


WITH daily_transports AS (
    SELECT
      region,
      service_date,

      -- Transport counts by service level (excluding Medicar, NEV, Standbys)
      COUNTIF(calltype_name = 'BLS' AND run_outcome = 'ran') AS bls_transports,
      COUNTIF(calltype_name = 'ALS' AND run_outcome = 'ran') AS als_transports,
      COUNTIF(calltype_name = 'CCT' AND run_outcome = 'ran') AS sct_transports,
      COUNTIF(calltype_name IN ('Flight Crew', 'Flight Crew Return Only') AND run_outcome = 'ran') AS flight_transports,

      -- Total transports (excluding Medicar, NEV, Standbys)
      COUNTIF(
        run_outcome = 'ran'
        AND calltype_name NOT IN ('Medicar', 'NEV - Wheelchair', 'Standby - ALS', 'Standby - BLS', 'Standby - Medicar')
      ) AS total_transports,

      -- Long distance transports (>50 miles)
      COUNTIF(
        run_outcome = 'ran'
        AND mileage > 50
        AND calltype_name NOT IN ('Medicar', 'NEV - Wheelchair', 'Standby - ALS', 'Standby - BLS', 'Standby - Medicar')
      ) AS ldt_over_50_miles,

      -- On-time performance
      COUNTIF(run_outcome = 'ran' AND is_on_time = TRUE) AS on_time_count,
      COUNTIF(run_outcome = 'ran' AND is_on_time IS NOT NULL) AS on_time_eligible_count,

      -- Time on task (excluding Medicar, NEV, Standbys to match clean_labor_hours denominator)
      SUM(CASE
        WHEN run_outcome = 'ran'
          AND calltype_name NOT IN ('Medicar', 'NEV - Wheelchair', 'Standby - ALS', 'Standby - BLS', 'Standby - Medicar')
        THEN time_on_task_minutes
        ELSE 0
      END) AS total_time_on_task_minutes,

      -- Cancelled and turned calls
      COUNTIF(run_outcome = 'cancelled') AS cancelled_calls,
      COUNTIF(run_outcome = 'turned') AS turned_calls

    FROM {{ref('bq_runs')}}
    WHERE service_date IS NOT NULL
      AND region IS NOT NULL
    GROUP BY region, service_date
  ),

  daily_shifts AS (
    SELECT
      s.region,
      s.shift_date,

      -- Ambulance counts (distinct units)
      COUNT(DISTINCT CASE WHEN EXTRACT(HOUR FROM s.shift_start) < 12 THEN s.unit_name END) AS ambulances_up_am,
      COUNT(DISTINCT CASE WHEN EXTRACT(HOUR FROM s.shift_start) >= 19 THEN s.unit_name END) AS ambulances_up_pm,
      COUNT(DISTINCT s.unit_name) AS total_ambulances,

      -- Clean labor hours = field shifts only, excluding training and orientation
      SUM(CASE
        WHEN s.is_field_shift = TRUE AND s.is_training = FALSE AND s.is_orientation = FALSE AND s.actual_hours_worked > 0
        THEN s.actual_hours_worked ELSE 0
      END) AS clean_labor_hours,

      -- Fully loaded hours = field + special event + orientation + training
      SUM(CASE
        WHEN (s.is_field_shift = TRUE OR s.is_special_event = TRUE OR s.is_orientation = TRUE OR s.is_training = TRUE) AND s.actual_hours_worked > 0
        THEN s.actual_hours_worked ELSE 0
      END) AS fully_loaded_hours,

      SUM(CASE WHEN s.is_special_event = TRUE THEN s.actual_hours_worked ELSE 0 END) AS special_event_hours,
      SUM(CASE WHEN s.is_orientation = TRUE OR s.is_training = TRUE THEN s.actual_hours_worked ELSE 0 END) AS orientation_hours,

      -- Special event hours by certification level
      SUM(CASE
        WHEN s.is_special_event = TRUE
          AND u.job_title IN ('EMT Basic', 'EMT - Basic', 'EMT - Advanced', 'Advanced EMT')
        THEN s.scheduled_hours ELSE 0
      END) AS emt_special_event_hours,

      SUM(CASE
        WHEN s.is_special_event = TRUE
          AND u.job_title IN ('Paramedic', 'Paramedic - Crit Care', 'Paramedic - Supervisor', 'Paramedic - Vent', 'Critical Care Paramedic')
        THEN s.scheduled_hours ELSE 0
      END) AS medic_special_event_hours

    FROM {{ref('bq_shifts')}} s
    LEFT JOIN {{ref('bq_users')}} u
      ON s.user_id = u.user_id AND s.source_database = u.source_database
    WHERE s.shift_date IS NOT NULL
      AND s.region IS NOT NULL
      AND s.is_assigned = TRUE
      AND s.region IN ('il', 'mi', 'tn_memphis', 'tn_nashville')
      AND (s.is_field_shift = TRUE OR s.is_special_event = TRUE OR s.is_orientation = TRUE)
    GROUP BY s.region, s.shift_date
  )

  SELECT
    COALESCE(t.region, s.region) AS region,

    CASE COALESCE(t.region, s.region)
      WHEN 'il' THEN 'Illinois'
      WHEN 'mi' THEN 'Michigan'
      WHEN 'tn_memphis' THEN 'Tennessee - Memphis'
      WHEN 'tn_nashville' THEN 'Tennessee - Nashville'
      ELSE COALESCE(t.region, s.region)
    END AS market_name,

    COALESCE(t.service_date, s.shift_date) AS report_date,

    CONCAT(
      FORMAT_DATE('%m/%d/%Y', COALESCE(t.service_date, s.shift_date)),
      '\n',
      FORMAT_DATE('%A', COALESCE(t.service_date, s.shift_date))
    ) AS report_date_display,

    -- Date dimensions
    EXTRACT(YEAR FROM COALESCE(t.service_date, s.shift_date)) AS report_year,
    EXTRACT(MONTH FROM COALESCE(t.service_date, s.shift_date)) AS report_month,
    FORMAT_DATE('%B %Y', COALESCE(t.service_date, s.shift_date)) AS report_month_name,
    FORMAT_DATE('%A', COALESCE(t.service_date, s.shift_date)) AS day_of_week,
    EXTRACT(DAYOFWEEK FROM COALESCE(t.service_date, s.shift_date)) AS day_of_week_num,

    -- Ambulance counts
    COALESCE(s.ambulances_up_am, 0) AS ambulances_up_am,
    COALESCE(s.ambulances_up_pm, 0) AS ambulances_up_pm,
    COALESCE(s.total_ambulances, 0) AS total_ambulances,

    -- Transport counts
    COALESCE(t.bls_transports, 0) AS bls_transports,
    COALESCE(t.als_transports, 0) AS als_transports,
    COALESCE(t.sct_transports, 0) AS sct_transports,
    COALESCE(t.flight_transports, 0) AS flight_transports,
    COALESCE(t.total_transports, 0) AS total_transports,
    COALESCE(t.ldt_over_50_miles, 0) AS ldt_over_50_miles,

    -- Hours
    COALESCE(s.fully_loaded_hours, 0) AS fully_loaded_hours,
    COALESCE(s.special_event_hours, 0) AS special_event_hours,
    COALESCE(s.orientation_hours, 0) AS orientation_hours,
    COALESCE(s.clean_labor_hours, 0) AS clean_labor_hours,
    COALESCE(s.emt_special_event_hours, 0) AS emt_special_event_hours,
    COALESCE(s.medic_special_event_hours, 0) AS medic_special_event_hours,

    -- On-time metrics
    COALESCE(t.on_time_count, 0) AS on_time_count,
    COALESCE(t.on_time_eligible_count, 0) AS on_time_eligible_count,
    SAFE_DIVIDE(t.on_time_count, t.on_time_eligible_count) AS on_time_percentage,

    -- Time on task
    COALESCE(t.total_time_on_task_minutes, 0) AS total_time_on_task_minutes,
    ROUND(
      SAFE_DIVIDE(
        COALESCE(t.total_time_on_task_minutes / 60.0, 0),
        s.clean_labor_hours
      ) * 2, 4
    ) AS time_on_task_percent,

    -- UHU (transports per unit hour: transports / labor hours * 2 crew per unit)
    SAFE_DIVIDE(t.total_transports, s.fully_loaded_hours) * 2 AS fully_loaded_uhu,
    SAFE_DIVIDE(t.total_transports, s.clean_labor_hours) * 2 AS clean_uhu,

    -- Reference metrics
    COALESCE(t.cancelled_calls, 0) AS cancelled_calls,
    COALESCE(t.turned_calls, 0) AS turned_calls

  FROM daily_transports t
  FULL OUTER JOIN daily_shifts s
    ON t.service_date = s.shift_date
    AND t.region = s.region