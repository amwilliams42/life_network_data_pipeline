{% macro generate_hourly_dow_stats(source_table) %}

WITH all_hours AS (
  SELECT generate_series(0, 23) AS hour_num
),
all_days AS (
  SELECT unnest(ARRAY['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']) AS day_of_week
),
all_combinations AS (
  SELECT
    LPAD(h.hour_num::text, 2, '0') || '00' AS hour_of_day,
    d.day_of_week
  FROM all_hours h
  CROSS JOIN all_days d
),
aggregated_data AS (
  SELECT
    r.hour_of_day,
    r.day_of_week,
    SUM(CASE WHEN r.run_outcome = 'ran' THEN 1 ELSE 0 END) AS ran_count,
    SUM(CASE WHEN r.run_outcome = 'turned' THEN 1 ELSE 0 END) AS turned_count,
    COUNT(*) AS call_count
  FROM {{ ref(source_table) }} AS r
  WHERE
    r.date_of_service >= CURRENT_DATE - INTERVAL '5 weeks'
    AND r.date_of_service < CURRENT_DATE
  GROUP BY
    r.hour_of_day,
    r.day_of_week
)
SELECT
  ac.hour_of_day,          -- x-axis categorical
  ac.day_of_week,          -- y-axis categorical
  COALESCE(CEIL(ad.ran_count::numeric / 5)::int, 0) AS avg_ran_count,
  COALESCE(CEIL(ad.turned_count::numeric / 5)::int, 0) AS avg_turned_count,
  COALESCE(CEIL(ad.call_count::numeric / 5)::int, 0) AS avg_call_count,
  CAST(ac.hour_of_day AS int) AS xsort,
  CASE lower(ac.day_of_week)
    WHEN 'sunday' THEN 1 WHEN 'monday' THEN 2 WHEN 'tuesday' THEN 3
    WHEN 'wednesday' THEN 4 WHEN 'thursday' THEN 5
    WHEN 'friday' THEN 6 WHEN 'saturday' THEN 7 ELSE 8
  END AS ysort
FROM all_combinations ac
LEFT JOIN aggregated_data ad
  ON ac.hour_of_day = ad.hour_of_day
  AND TRIM(ac.day_of_week) = TRIM(ad.day_of_week)
ORDER BY
  CASE lower(ac.day_of_week)
    WHEN 'sunday'    THEN 1
    WHEN 'monday'    THEN 2
    WHEN 'tuesday'   THEN 3
    WHEN 'wednesday' THEN 4
    WHEN 'thursday'  THEN 5
    WHEN 'friday'    THEN 6
    WHEN 'saturday'  THEN 7
    ELSE 8
  END,
  xsort

{% endmacro %}