{{ config(materialized='view') }}

{%- set dataset_configs = [
  {'dataset': 'traumasoft_tn', 'suffix': 'tn'},
  {'dataset': 'traumasoft_mi', 'suffix': 'mi'},
  {'dataset': 'traumasoft_il', 'suffix': 'il'}
] -%}

with
    {%- for config in dataset_configs %}
        {{ config.suffix }}_qa as (
            SELECT
               leg.run_number,
               rev.leg_id,
               qa.status_date        as qa_status_date,
               qa.status_id          as qa_status_id,
               status.status_name    as qa_status_name,
               qa.set_by_user        as qa_set_by_user,
               qa.return_reason_id,
               qa.review_reason_id,
               qa.notes              as qa_notes,
               qa.send_notes_to_crew,
               '{{ config.suffix }}' as source_database

            FROM {{ source(config.dataset, 'cad_trip_legs') }} as leg
                     INNER JOIN {{ source(config.dataset, 'cad_trip_legs_rev') }} as rev
                                on leg.id = rev.leg_id AND leg.rev = rev.rev
                     left join {{ source(config.dataset, 'epcr_v2_cad_legs') }} as epcr_legs
                               on epcr_legs.cad_leg_id = leg.id
                     left join {{ source(config.dataset, 'epcr_v2_runs') }} as runs
                               on epcr_legs.run_id = runs.id
                     LEFT JOIN {{ source(config.dataset, 'sched_unit_types') }} as calltype
                               ON calltype.id = rev.calltype_id
                     LEFT JOIN {{ source(config.dataset, 'ibd_level_of_service') }} as los
                               ON los.id = rev.los_id AND los.call_type_id = calltype_id
                     LEFT JOIN {{ source(config.dataset, 'cad_sources') }} AS source
                               ON source.id = rev.source_id
                     LEFT JOIN {{ source(config.dataset, 'ibd_subzones') }} as subzones
                               ON rev.response_zone_id = subzones.subzone_id
                     LEFT JOIN {{ source(config.dataset, 'epcr_v2_qaqr_run_status') }} as qa
                               ON runs.id = qa.run_id AND qa.removed = 0
                     LEFT JOIN {{ source(config.dataset, 'epcr_v2_qaqr_statuses') }} as status
                               ON qa.status_id = status.id) {{ "," if not loop.last }}
{%- endfor %}

{%- for config in dataset_configs %}
select * from {{ config.suffix }}_qa
{%- if not loop.last %}
union all
{% endif %}
{%- endfor %}
