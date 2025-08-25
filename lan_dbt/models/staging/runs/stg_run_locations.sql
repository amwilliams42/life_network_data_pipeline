{{ config(materialized='view') }}

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

with {% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
    {{ suffix }}_locations as (
        SELECT 
            rev.leg_id,
            rev.pu_facility_name as pickup_facility,
            rev.pu_address1 as pickup_address1,
            rev.pu_city as pickup_city,
            rev.pu_state as pickup_state,
            rev.pu_zipcode as pickup_zipcode,
            rev.pu_lat as pickup_latitude,
            rev.pu_lon as pickup_longitude,
            rev.do_facility_name as dropoff_facility,
            rev.do_address1 as dropoff_address1,
            rev.do_city as dropoff_city,
            rev.do_state as dropoff_state,
            rev.do_zipcode as dropoff_zipcode,
            rev.do_lat as dropoff_latitude,
            rev.do_lon as dropoff_longitude,
            rev.distance_meters,
            rev.mileage,
            '{{ suffix }}' as source_database
        FROM {{ source(dataset, 'cad_trip_legs') }} as leg
        INNER JOIN {{ source(dataset, 'cad_trip_legs_rev') }} as rev
            ON leg.id = rev.leg_id AND leg.rev = rev.rev
    ),
{% endfor %}

combined as (
    {% for dataset in datasets %}
    {% set suffix=dataset.split('_')[1] %}
    select * from {{ suffix }}_locations
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select * from combined