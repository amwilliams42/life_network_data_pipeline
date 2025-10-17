{{ config(materialized='view') }}

WITH
    -- Get all distinct dates from the schedule (limited to last 5 weeks for performance)
    all_dates AS (
        SELECT DISTINCT date_line
        FROM {{ ref('stg_schedule') }}
        WHERE date_line >= current_date - interval '5 weeks'
        AND date_line < current_date + interval '1 day'
    ),

    -- Define specific cost centers to include
    cost_center_list AS (
        SELECT cost_center_id, cost_center_name, source_database, special_event
        FROM (VALUES
            (29, 'Orientation', 'il', false),
            (6, 'EMT Carol Stream', 'il', false),
            (10, 'EMT Chicago', 'il', false),
            (22, 'EMT Oakland County', 'mi', false),
            (3, 'EMT Skokie', 'il', false),
            (20, 'EMT Wayne County', 'mi', false),
            (11, 'Lincoln Park Rescue', 'mi', false),
            (70, 'Memp - Critical Care ', 'tn', false),
            (47, 'Memp - EMT BLS', 'tn', false),
            (63, 'Memp - LDT', 'tn', false),
            (52, 'Memp - Paramedic ALS', 'tn', false),
            (14, 'Memp - Special Events', 'tn', true),
            (48, 'Miss - EMT BLS', 'tn', false),
            (53, 'Miss - Paramedic ALS', 'tn', false),
            (49, 'Nash - EMT BLS', 'tn', false),
            (64, 'Nash - LDT', 'tn', false),
            (54, 'Nash - Paramedic ALS', 'tn', false),
            (56, 'Nash - Special Events', 'tn', true),
            (8, 'Paramedic Chicago', 'il', false),
            (23, 'Paramedic Oakland County', 'mi', false),
            (4, 'Paramedic Skokie', 'il', false),
            (21, 'Paramedic Wayne County', 'mi', false),
            (27, 'Special Events', 'il', true)
        ) AS t(cost_center_id, cost_center_name, source_database, special_event)
    ),

    -- Get all distinct cost centers from the defined list
    all_cost_centers AS (
        SELECT
            cost_center_id,
            cost_center_name,
            source_database,
            special_event
        FROM cost_center_list
    ),

    -- Create a cross-join of all dates and all cost centers
    date_cost_center_combinations AS (
        SELECT
            d.date_line,
            cc.cost_center_id,
            cc.cost_center_name,
            cc.source_database,
            cc.special_event
        FROM all_dates d
        CROSS JOIN all_cost_centers cc
    ),

    -- Aggregate actual hours by date and cost center
    actual_hours AS (
        SELECT
            date_line,
            cost_center_id,
            cost_center_name,
            source_database,
            is_training,
            ROUND(SUM(scheduled_hours), 2) AS scheduled_hours,
            ROUND(SUM(open_hours), 2) AS open_hours,
            ROUND(SUM(COALESCE(hours_difference, 0)), 2) AS worked_hours
        FROM {{ ref('stg_schedule') }}
        WHERE cost_center_id IS NOT NULL
        GROUP BY
            date_line,
            cost_center_id,
            cost_center_name,
            source_database,
            is_training
    )

-- Join the cross join with actual hours, filling nulls with 0
SELECT
    dcc.date_line,
    dcc.cost_center_id,
    dcc.cost_center_name,
    dcc.source_database,
    dcc.special_event,
    ah.is_training,
    COALESCE(ah.scheduled_hours, 0) AS scheduled_hours,
    COALESCE(ah.open_hours, 0) AS open_hours,
    GREATEST(COALESCE(ah.worked_hours, 0),0) AS worked_hours
FROM date_cost_center_combinations dcc
LEFT JOIN actual_hours ah
    ON dcc.date_line = ah.date_line
    AND dcc.cost_center_id = ah.cost_center_id
    AND dcc.source_database = ah.source_database
ORDER BY
    dcc.source_database,
    dcc.cost_center_name,
    dcc.date_line
