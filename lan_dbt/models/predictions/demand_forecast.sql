{{ config(materialized='view') }}

-- This model references the predictions.demand_forecast table
-- which is populated by the Prefect demand_forecast flow.
-- The flow runs Prophet forecasting and writes results to this table.

SELECT
    date_of_service,
    region,
    predicted_calls,
    predicted_calls_lower,
    predicted_calls_upper,
    trend,
    weekly as weekly_seasonality,
    yearly as yearly_seasonality,
    (is_weekend::int)::boolean as is_weekend,
    -- Handle both old and new holiday column structures
    COALESCE(major_holiday_effect, holiday_effect, 0) as holiday_effect,
    COALESCE(minor_holiday_effect, 0) as minor_holiday_effect,
    COALESCE(day_before_major_holiday_effect, working_day_before_effect, 0) as day_before_holiday_effect,
    COALESCE(day_after_major_holiday_effect, day_after_holiday_effect, 0) as day_after_holiday_effect
FROM {{ source('predictions', 'demand_forecast') }}
ORDER BY region, date_of_service