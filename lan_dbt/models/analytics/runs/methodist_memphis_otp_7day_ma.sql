{{ config(materialized='view') }}

with filtered_runs as (
    SELECT
        date_of_service,
        emergency,
        methodist_response_time_minutes,
        is_on_time
    FROM {{ ref('methodist_memphis_on_time_performance') }}
    WHERE methodist_response_time_minutes IS NOT NULL
      AND methodist_response_time_minutes >= 0
      AND methodist_response_time_minutes <= 120
),

daily_performance as (
    SELECT
        date_of_service,
        emergency,
        COUNT(*) as total_calls,
        SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) as on_time_calls,
        ROUND(100.0 * SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) / COUNT(*), 2) as on_time_percentage
    FROM filtered_runs
    GROUP BY date_of_service, emergency
),

daily_combined as (
    SELECT
        date_of_service,
        COUNT(*) as total_calls,
        SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) as on_time_calls,
        ROUND(100.0 * SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) / COUNT(*), 2) as on_time_percentage
    FROM filtered_runs
    GROUP BY date_of_service
),

moving_average as (
    SELECT
        date_of_service,
        emergency,
        total_calls,
        on_time_calls,
        on_time_percentage,
        ROUND(
            AVG(on_time_percentage) OVER (
                PARTITION BY emergency
                ORDER BY date_of_service
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            2
        ) as on_time_percentage_7day_ma,
        COUNT(*) OVER (
            PARTITION BY emergency
            ORDER BY date_of_service
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as days_in_window
    FROM daily_performance
),

combined_moving_average as (
    SELECT
        date_of_service,
        total_calls,
        on_time_calls,
        on_time_percentage,
        ROUND(
            AVG(on_time_percentage) OVER (
                ORDER BY date_of_service
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            2
        ) as on_time_percentage_7day_ma,
        COUNT(*) OVER (
            ORDER BY date_of_service
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as days_in_window
    FROM daily_combined
),

by_call_type as (
    SELECT
        date_of_service,
        CASE WHEN emergency = 1 THEN 'Emergency' ELSE 'Non-Emergency' END as call_type,
        total_calls,
        on_time_calls,
        on_time_percentage,
        on_time_percentage_7day_ma,
        days_in_window
    FROM moving_average
),

combined as (
    SELECT
        date_of_service,
        'Combined' as call_type,
        total_calls,
        on_time_calls,
        on_time_percentage,
        on_time_percentage_7day_ma,
        days_in_window
    FROM combined_moving_average
)

SELECT * FROM by_call_type
UNION ALL
SELECT * FROM combined
ORDER BY date_of_service, call_type