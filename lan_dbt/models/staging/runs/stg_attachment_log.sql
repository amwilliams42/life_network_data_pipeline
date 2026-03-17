{{ config(materialized='view') }}

/*
    Staging model for attachment log data.

    Joins attachments_log to attachments and cad_trip_leg_attachments
    to get attachment events linked to specific trip legs.

    Only includes 'create' actions (initial uploads).
*/

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

WITH {% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
{{ suffix }}_attachment_log AS (
    SELECT
        att_log.id AS log_id,
        att_log.attachment_id,
        att_log.user_id,
        att_log.action,
        att_log.timestamp AS action_timestamp,
        att.file_name,
        att.date AS attachment_date,
        leg_att.id AS leg_attachment_id,
        leg_att.leg_id,
        '{{ suffix }}' AS source_database
    FROM {{ source(dataset, 'attachments_log') }} AS att_log
    INNER JOIN {{ source(dataset, 'attachments') }} AS att
        ON att.id = att_log.attachment_id
    INNER JOIN {{ source(dataset, 'cad_trip_leg_attachments') }} AS leg_att
        ON leg_att.attachment_id = att_log.attachment_id
    WHERE att_log.action = 'create'
){% if not loop.last %},{% endif %}
{% endfor %},

-- Get attachment types for each leg attachment
{% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
{{ suffix }}_attachment_types AS (
    SELECT
        leg_att_type.trip_leg_attachment_id AS leg_attachment_id,
        STRING_AGG(DISTINCT att_type.name, ', ' ORDER BY att_type.name) AS attachment_types,
        '{{ suffix }}' AS source_database
    FROM {{ source(dataset, 'cad_trip_leg_attachment_types') }} AS leg_att_type
    LEFT JOIN {{ source(dataset, 'ibd_attachment_types') }} AS att_type
        ON att_type.type_id = leg_att_type.type_id
    GROUP BY leg_att_type.trip_leg_attachment_id
){% if not loop.last %},{% endif %}
{% endfor %},

all_attachment_log AS (
    SELECT * FROM tn_attachment_log
    UNION ALL
    SELECT * FROM mi_attachment_log
    UNION ALL
    SELECT * FROM il_attachment_log
),

all_attachment_types AS (
    SELECT * FROM tn_attachment_types
    UNION ALL
    SELECT * FROM mi_attachment_types
    UNION ALL
    SELECT * FROM il_attachment_types
)

SELECT
    al.log_id,
    al.attachment_id,
    al.user_id,
    al.action,
    al.action_timestamp,
    al.file_name,
    al.attachment_date,
    al.leg_attachment_id,
    al.leg_id,
    al.source_database,
    COALESCE(at.attachment_types, 'Unspecified') AS attachment_types
FROM all_attachment_log al
LEFT JOIN all_attachment_types at
    ON al.leg_attachment_id = at.leg_attachment_id
    AND al.source_database = at.source_database