{{ config(materialized='view') }}

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

{% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}

SELECT
    id AS pay_period_id,
    number AS pay_period_number,
    start::date AS start_date,
    "end"::date AS end_date,
    weeks_in_payperiod,
    closed = 1 AS is_closed,
    date_closed,
    '{{ suffix }}' AS source_database
FROM {{ source(dataset, 'sched_pay_periods') }}

{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}