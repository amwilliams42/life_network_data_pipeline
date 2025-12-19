{{ config(materialized='table') }}

WITH mem_daily AS (
  SELECT
    date_of_service,
    'mem' as region,
    COUNT(*) as total_calls,
    SUM(CASE WHEN run_outcome = 'ran' THEN 1 ELSE 0 END) as ran_count,
    SUM(CASE WHEN run_outcome = 'turned' THEN 1 ELSE 0 END) as turned_count,
    SUM(CASE WHEN run_outcome = 'cancelled' THEN 1 ELSE 0 END) as cancelled_count,
    SUM(CASE WHEN run_outcome IN ('ran', 'turned') THEN 1 ELSE 0 END) as demand_calls
  FROM {{ ref('int_mem_runs') }}
  WHERE date_of_service IS NOT NULL
  GROUP BY date_of_service
),
il_daily AS (
  SELECT
    date_of_service,
    'il' as region,
    COUNT(*) as total_calls,
    SUM(CASE WHEN run_outcome = 'ran' THEN 1 ELSE 0 END) as ran_count,
    SUM(CASE WHEN run_outcome = 'turned' THEN 1 ELSE 0 END) as turned_count,
    SUM(CASE WHEN run_outcome = 'cancelled' THEN 1 ELSE 0 END) as cancelled_count,
    SUM(CASE WHEN run_outcome IN ('ran', 'turned') THEN 1 ELSE 0 END) as demand_calls
  FROM {{ ref('int_il_runs') }}
  WHERE date_of_service IS NOT NULL
  GROUP BY date_of_service
),
mi_daily AS (
  SELECT
    date_of_service,
    'mi' as region,
    COUNT(*) as total_calls,
    SUM(CASE WHEN run_outcome = 'ran' THEN 1 ELSE 0 END) as ran_count,
    SUM(CASE WHEN run_outcome = 'turned' THEN 1 ELSE 0 END) as turned_count,
    SUM(CASE WHEN run_outcome = 'cancelled' THEN 1 ELSE 0 END) as cancelled_count,
    SUM(CASE WHEN run_outcome IN ('ran', 'turned') THEN 1 ELSE 0 END) as demand_calls
  FROM {{ ref('int_mi_runs') }}
  WHERE date_of_service IS NOT NULL
  GROUP BY date_of_service
),
nash_daily AS (
  SELECT
    date_of_service,
    'nash' as region,
    COUNT(*) as total_calls,
    SUM(CASE WHEN run_outcome = 'ran' THEN 1 ELSE 0 END) as ran_count,
    SUM(CASE WHEN run_outcome = 'turned' THEN 1 ELSE 0 END) as turned_count,
    SUM(CASE WHEN run_outcome = 'cancelled' THEN 1 ELSE 0 END) as cancelled_count,
    SUM(CASE WHEN run_outcome IN ('ran', 'turned') THEN 1 ELSE 0 END) as demand_calls
  FROM {{ ref('int_nash_runs') }}
  WHERE date_of_service IS NOT NULL
  GROUP BY date_of_service
),
combined AS (
  SELECT * FROM mem_daily
  UNION ALL
  SELECT * FROM il_daily
  UNION ALL
  SELECT * FROM mi_daily
  UNION ALL
  SELECT * FROM nash_daily
)
SELECT
  date_of_service,
  region,
  total_calls,
  demand_calls,  -- Actual demand (ran + turned, excludes cancelled)
  ran_count,
  turned_count,
  cancelled_count,
  -- Additional date features for analysis
  EXTRACT(DOW FROM date_of_service) as day_of_week_num,
  TO_CHAR(date_of_service, 'Day') as day_of_week_name,
  EXTRACT(MONTH FROM date_of_service) as month,
  EXTRACT(YEAR FROM date_of_service) as year,
  CASE
    WHEN EXTRACT(DOW FROM date_of_service) IN (0, 6) THEN true
    ELSE false
  END as is_weekend
FROM combined
ORDER BY region, date_of_service