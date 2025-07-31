{{ config(materialized='table') }}

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

with {% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
    {{ suffix }}_runs as (SELECT rev.leg_id,
    DATE (rev.pickup_time) as date_of_service
   , calltype.name as calltype_name
   , source.id as source_id
   , source.name as source_name
   , los.name as level_of_service
   , subzones.name as market
   , rev.pu_facility_name as pickup_facility
   , rev.do_facility_name as dropoff_facility
   , '{{ suffix }}' as source_database
FROM
    {{ source(dataset, 'cad_trip_legs') }} as leg
    INNER JOIN {{ source(dataset, 'cad_trip_legs_rev') }} as rev
on leg.id = rev.leg_id AND leg.rev = rev.rev
    LEFT JOIN {{ source(dataset, 'sched_unit_types') }} as calltype ON calltype.id = rev.calltype_id
    LEFT JOIN {{ source( dataset, 'ibd_level_of_service') }} as los ON los.id = rev.los_id AND los.call_type_id = calltype_id
    LEFT JOIN {{ source( dataset, 'cad_sources') }} AS source ON source.id = rev.source_id
    LEFT JOIN {{ source( dataset, 'ibd_subzones') }} as subzones ON rev.response_zone_id = subzones.subzone_id
    ),
    {%  endfor %}

combined as (
    select * from tn_runs
    union all
    select * from mi_runs
    union all
    select * from il_runs
)
select * from combined