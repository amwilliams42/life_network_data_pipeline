{{ config(materialized='table') }}

WITH forecast AS (
  SELECT
    date_of_service,
    region,
    predicted_calls,
    predicted_calls_lower,
    predicted_calls_upper,
    trend,
    weekly_seasonality,
    yearly_seasonality,
    is_weekend,
    holiday_effect,
    COALESCE(minor_holiday_effect, 0) as minor_holiday_effect,
    COALESCE(day_before_holiday_effect, 0) as day_before_holiday_effect,
    COALESCE(day_after_holiday_effect, 0) as day_after_holiday_effect
  FROM {{ ref('demand_forecast') }}
),
actuals AS (
  SELECT
    date_of_service,
    region,
    demand_calls as actual_demand,  -- Actual demand (ran + turned)
    total_calls as actual_total_calls,
    ran_count as actual_ran,
    turned_count as actual_turned,
    cancelled_count as actual_cancelled
  FROM {{ ref('int_daily_demand_history') }}
),
combined AS (
  SELECT
    COALESCE(f.date_of_service, a.date_of_service) as date_of_service,
    COALESCE(f.region, a.region) as region,
    -- Forecast data
    f.predicted_calls,
    f.predicted_calls_lower,
    f.predicted_calls_upper,
    f.trend,
    f.weekly_seasonality,
    f.yearly_seasonality,
    f.is_weekend,
    f.holiday_effect,
    f.minor_holiday_effect,
    f.day_before_holiday_effect,
    f.day_after_holiday_effect,
    -- Actual data
    a.actual_demand,
    a.actual_total_calls,
    a.actual_ran,
    a.actual_turned,
    a.actual_cancelled,
    -- Prediction error metrics (only for historical dates)
    CASE
      WHEN a.actual_demand IS NOT NULL
      THEN ABS(f.predicted_calls - a.actual_demand)
    END as absolute_error,
    CASE
      WHEN a.actual_demand IS NOT NULL AND a.actual_demand > 0
      THEN ABS(f.predicted_calls - a.actual_demand)::float / a.actual_demand * 100
    END as percentage_error,
    CASE
      WHEN a.actual_demand IS NOT NULL
      THEN f.predicted_calls - a.actual_demand
    END as forecast_bias,
    -- Flag for forecast vs historical
    CASE
      WHEN a.actual_demand IS NULL THEN true
      ELSE false
    END as is_future_forecast,
    -- Within confidence interval check
    CASE
      WHEN a.actual_demand IS NOT NULL
      THEN (a.actual_demand BETWEEN f.predicted_calls_lower AND f.predicted_calls_upper)
    END as within_confidence_interval
  FROM forecast f
  FULL OUTER JOIN actuals a
    ON f.date_of_service = a.date_of_service
    AND f.region = a.region
)
SELECT
  date_of_service,
  region,
  predicted_calls,
  predicted_calls_lower,
  predicted_calls_upper,
  actual_demand,
  actual_total_calls,
  actual_ran,
  actual_turned,
  actual_cancelled,
  -- Accuracy metrics
  absolute_error,
  percentage_error,
  forecast_bias,
  within_confidence_interval,
  -- Decomposition components
  trend,
  weekly_seasonality,
  yearly_seasonality,
  -- Special day indicators
  is_weekend,
  holiday_effect,
  minor_holiday_effect,
  day_before_holiday_effect,
  day_after_holiday_effect,
  -- Metadata
  is_future_forecast,
  -- Date parts for easy filtering
  EXTRACT(YEAR FROM date_of_service) as year,
  EXTRACT(MONTH FROM date_of_service) as month,
  EXTRACT(DOW FROM date_of_service) as day_of_week_num,
  TO_CHAR(date_of_service, 'Day') as day_of_week_name
FROM combined
ORDER BY region, date_of_service