{{ config(materialized='view') }}

-- Diagnostic view to analyze forecast accuracy and model performance

WITH historical_only AS (
  SELECT *
  FROM {{ ref('demand_forecast_analysis') }}
  WHERE is_future_forecast = false
    AND actual_demand IS NOT NULL
),
regional_metrics AS (
  SELECT
    region,
    COUNT(*) as total_days,

    -- Accuracy metrics
    ROUND(AVG(absolute_error)::numeric, 2) as mean_absolute_error,
    ROUND(AVG(percentage_error)::numeric, 2) as mean_percentage_error,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY absolute_error)::numeric, 2) as median_absolute_error,

    -- Bias analysis
    ROUND(AVG(forecast_bias)::numeric, 2) as mean_bias,
    SUM(CASE WHEN forecast_bias > 0 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as pct_over_forecast,
    SUM(CASE WHEN forecast_bias < 0 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as pct_under_forecast,

    -- Confidence interval performance
    SUM(CASE WHEN within_confidence_interval THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as pct_within_ci,

    -- Actual demand stats
    ROUND(AVG(actual_demand)::numeric, 2) as avg_actual_demand,
    ROUND(STDDEV(actual_demand)::numeric, 2) as stddev_actual_demand,
    MIN(actual_demand) as min_actual_demand,
    MAX(actual_demand) as max_actual_demand,

    -- Predicted demand stats
    ROUND(AVG(predicted_calls)::numeric, 2) as avg_predicted_demand,
    ROUND(AVG(predicted_calls_upper - predicted_calls_lower)::numeric, 2) as avg_ci_width

  FROM historical_only
  GROUP BY region
),
day_of_week_metrics AS (
  SELECT
    day_of_week_name,
    day_of_week_num,
    COUNT(*) as total_observations,
    ROUND(AVG(absolute_error)::numeric, 2) as mean_absolute_error,
    ROUND(AVG(percentage_error)::numeric, 2) as mean_percentage_error,
    SUM(CASE WHEN within_confidence_interval THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as pct_within_ci
  FROM historical_only
  GROUP BY day_of_week_name, day_of_week_num
),
weekend_vs_weekday AS (
  SELECT
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    COUNT(*) as total_observations,
    ROUND(AVG(absolute_error)::numeric, 2) as mean_absolute_error,
    ROUND(AVG(percentage_error)::numeric, 2) as mean_percentage_error,
    SUM(CASE WHEN within_confidence_interval THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as pct_within_ci
  FROM historical_only
  GROUP BY CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END
)

-- Return all metrics
SELECT
  'Regional Performance' as metric_type,
  region as category,
  NULL::text as subcategory,
  total_days as n,
  mean_absolute_error as mae,
  mean_percentage_error as mape,
  mean_bias as bias,
  pct_within_ci as ci_coverage,
  avg_actual_demand,
  avg_predicted_demand,
  avg_ci_width
FROM regional_metrics

UNION ALL

SELECT
  'Day of Week Performance' as metric_type,
  day_of_week_name as category,
  NULL::text as subcategory,
  total_observations as n,
  mean_absolute_error as mae,
  mean_percentage_error as mape,
  NULL::numeric as bias,
  pct_within_ci as ci_coverage,
  NULL::numeric as avg_actual_demand,
  NULL::numeric as avg_predicted_demand,
  NULL::numeric as avg_ci_width
FROM day_of_week_metrics

UNION ALL

SELECT
  'Weekend vs Weekday' as metric_type,
  day_type as category,
  NULL::text as subcategory,
  total_observations as n,
  mean_absolute_error as mae,
  mean_percentage_error as mape,
  NULL::numeric as bias,
  pct_within_ci as ci_coverage,
  NULL::numeric as avg_actual_demand,
  NULL::numeric as avg_predicted_demand,
  NULL::numeric as avg_ci_width
FROM weekend_vs_weekday

ORDER BY metric_type, category