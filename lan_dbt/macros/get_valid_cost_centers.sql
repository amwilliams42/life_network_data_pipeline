{% macro get_valid_cost_centers() %}
valid_cost_centers as (
        select cost_center_id, cost_center_name, source_database, special_event
        from (values
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
        ) as t(cost_center_id, cost_center_name, source_database, special_event)
    )
{% endmacro %}
