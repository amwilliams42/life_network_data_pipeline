{{ config(materialized='view') }}

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

with {% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
    {{ suffix }}_qa_export as (
        SELECT
            run.id as run_id,
            run.run_num,
            date(trip.trip_date) as date_of_service,
            patient.last_name || ', ' || patient.first_name as patient_name,
            patient.dob as dob,
            calltype.name || ' ' || los.name as calltype,
            run.finalized,
            date(run.finalize_date) as finalized_date,
            qa_statuses.status_name,
            date(epcr_run_status.status_date) as qa_date,
            reviewer.last_name || ', ' || reviewer.first_name as reviewer_name,
            lead_crew.last_name|| ', ' ||lead_crew.first_name as crew_name,
            CASE WHEN t1.run_id IS NOT NULL THEN TRUE ELSE FALSE END as exported,
            CASE
                WHEN t1.run_id IS NULL THEN NULL
                WHEN t1.errors IS NULL THEN TRUE
                ELSE FALSE
            END as success,
            t1.errors,
            '{{ suffix }}' as source_database
        FROM
            {{ source(dataset, 'cad_trip_legs') }} leg
            LEFT JOIN {{ source(dataset, 'cad_trips') }} as trip on trip.id = leg.trip_id
            LEFT JOIN {{ source(dataset, 'cad_trip_legs_rev') }} as rev on rev.leg_id = leg.id AND rev.rev = leg.rev
            LEFT JOIN {{ source(dataset, 'epcr_v2_cad_legs') }} as epcr_leg on epcr_leg.cad_leg_id = leg.id
            LEFT JOIN {{ source(dataset, 'epcr_v2_runs') }} as run on run.id = epcr_leg.run_id
            LEFT JOIN {{ source(dataset, 'epcr_v2_qaqr_run_status') }} as epcr_run_status on epcr_run_status.run_id = run.id
            LEFT JOIN {{ source(dataset, 'epcr_v2_qaqr_statuses') }} as qa_statuses on epcr_run_status.status_id = qa_statuses.id
            LEFT JOIN {{ source(dataset, 'users') }} as reviewer on epcr_run_status.set_by_user = reviewer.user_id
            LEFT JOIN {{ source(dataset, 'users') }} as lead_crew on run.create_user = lead_crew.user_id
            LEFT JOIN {{ source(dataset, 'epcr_v3_export_trigger_log') }} as t1 on run.id = t1.run_id
            LEFT JOIN {{ source(dataset, 'ibd_level_of_service') }} as los on rev.los_id = los.id
            LEFT JOIN {{ source(dataset, 'sched_unit_types') }} as calltype on calltype.id = rev.calltype_id
            LEFT JOIN {{ source(dataset, 'ibd_patients') }} as patient on trip.patient_id = patient.patient_id
    ),
{% endfor %}

combined as (
    select * from tn_qa_export
    union all
    select * from mi_qa_export
    union all
    select * from il_qa_export
)

select * from combined