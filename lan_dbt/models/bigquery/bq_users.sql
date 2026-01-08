{{ config(materialized='table') }}

/*
    BigQuery Users Dimension

    User dimension table for PowerBI reporting.
    Join to fact tables (bq_shifts, bq_transports) via user_id + source_database.
*/

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

{% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
{% if loop.first %}WITH {% endif %}{{ suffix }}_users AS (
    SELECT
        u.user_id,
        '{{ suffix }}' AS source_database,
        CASE
            WHEN '{{ suffix }}' = 'il' THEN 'Illinois'
            WHEN '{{ suffix }}' = 'mi' THEN 'Michigan'
            WHEN '{{ suffix }}' = 'tn' THEN 'Tennessee'
            ELSE '{{ suffix }}'
        END AS state,
        u.employee_num,
        u.first_name,
        u.last_name,
        CASE
            WHEN u.first_name IS NOT NULL AND u.last_name IS NOT NULL
            THEN CONCAT(u.first_name, ' ', u.last_name)
            ELSE NULL
        END AS full_name,
        CASE
            WHEN u.first_name IS NOT NULL AND u.last_name IS NOT NULL
            THEN CONCAT(u.last_name, ', ', u.first_name)
            ELSE NULL
        END AS display_name,
        u.job_title_id,
        jt.name AS job_title,
        u.email_address,
        CASE WHEN u.disabled = '1' OR u.disabled = 'true' THEN true ELSE false END AS is_disabled,
        CASE WHEN u.deactivated = 1 THEN true ELSE false END AS is_deactivated,
        CASE
            WHEN (u.disabled = '0' OR u.disabled = 'false' OR u.disabled IS NULL)
                AND u.deactivated = 0
            THEN true
            ELSE false
        END AS is_active
    FROM {{ source(dataset, 'users') }} u
    LEFT JOIN {{ source(dataset, 'user_job_titles') }} jt
        ON jt.id = u.job_title_id
){% if not loop.last %},{% endif %}
{% endfor %}

SELECT * FROM tn_users
UNION ALL
SELECT * FROM mi_users
UNION ALL
SELECT * FROM il_users