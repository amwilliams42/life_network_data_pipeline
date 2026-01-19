{{ config(materialized='view') }}

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

with {% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
    {{ suffix }}_timestamps as (
        SELECT 
            leg.run_number,
            rev.leg_id,
            rev.leg_date as service_date,
            
            -- Core Scheduling Times
            rev.pickup_time,
            rev.orig_pickup_time,
            rev.requested_pickup_time,
            rev.appt_time as appointment_time,
            rev.return_time,
            rev.ready_now,
            
            -- Dispatch Times
            rev.call_started_date,
            rev.late_dispatch_created,
            rev.late_dispatch_time,
            rev.company_assignment_timestamp,
            
            -- Operational Status Times (Core EMS Timeline)
            rev.assigned_time,
            rev.acknowledged_time,
            rev.enroute_time,
            rev.at_scene_time,
            rev.transporting_time,
            rev.at_destination_time,
            rev.clear_time,
            rev.canceled_time,
            
            -- Additional Status Times
            rev.last_status_timestamp,
            rev.requested_pickup_time as requested_time,
            rev.atpatientbs_time as at_patient_bedside_time,
            
            -- Record Management Times
            rev.last_modified_date,
            rev.modified as record_modified,
            
            -- Calculated Durations (in minutes)
            CASE 
                WHEN rev.assigned_time IS NOT NULL AND rev.call_started_date IS NOT NULL
                THEN EXTRACT(EPOCH FROM (rev.assigned_time - rev.call_started_date))/60.0
            END as call_to_assignment_minutes,
            
            CASE 
                WHEN rev.acknowledged_time IS NOT NULL AND rev.assigned_time IS NOT NULL
                THEN EXTRACT(EPOCH FROM (rev.acknowledged_time - rev.assigned_time))/60.0
            END as assignment_to_ack_minutes,
            
            CASE 
                WHEN rev.enroute_time IS NOT NULL AND rev.acknowledged_time IS NOT NULL
                THEN EXTRACT(EPOCH FROM (rev.enroute_time - rev.acknowledged_time))/60.0
            END as ack_to_enroute_minutes,
            
            CASE 
                WHEN rev.at_scene_time IS NOT NULL AND rev.enroute_time IS NOT NULL
                THEN EXTRACT(EPOCH FROM (rev.at_scene_time - rev.enroute_time))/60.0
            END as enroute_to_scene_minutes,
            
            CASE 
                WHEN rev.transporting_time IS NOT NULL AND rev.at_scene_time IS NOT NULL
                THEN EXTRACT(EPOCH FROM (rev.transporting_time - rev.at_scene_time))/60.0
            END as scene_time_minutes,
            
            CASE 
                WHEN rev.at_destination_time IS NOT NULL AND rev.transporting_time IS NOT NULL
                THEN EXTRACT(EPOCH FROM (rev.at_destination_time - rev.transporting_time))/60.0
            END as transport_time_minutes,
            
            CASE 
                WHEN rev.clear_time IS NOT NULL AND rev.at_destination_time IS NOT NULL
                THEN EXTRACT(EPOCH FROM (rev.clear_time - rev.at_destination_time))/60.0
            END as destination_to_clear_minutes,
            
            -- Total Times
            CASE 
                WHEN rev.clear_time IS NOT NULL AND rev.call_started_date IS NOT NULL
                THEN EXTRACT(EPOCH FROM (rev.clear_time - rev.call_started_date))/60.0
            END as total_call_duration_minutes,
            
            CASE 
                WHEN rev.clear_time IS NOT NULL AND rev.assigned_time IS NOT NULL
                THEN EXTRACT(EPOCH FROM (rev.clear_time - rev.assigned_time))/60.0
            END as total_unit_time_minutes,
            
            -- Response Time (Call to Scene)
            CASE 
                WHEN rev.at_scene_time IS NOT NULL AND rev.acknowledged_time IS NOT NULL
                THEN EXTRACT(EPOCH FROM (rev.at_scene_time - rev.call_started_date))/60.0
            END as response_time_minutes,

            -- Time Categories for Analytics
            EXTRACT(HOUR FROM rev.pickup_time) as pickup_hour,
            EXTRACT(DOW FROM rev.pickup_time) as pickup_day_of_week,
            DATE_PART('week', rev.pickup_time) as pickup_week_of_year,
            
            '{{ suffix }}' as source_database
            
        FROM {{ source(dataset, 'cad_trip_legs') }} as leg
        INNER JOIN {{ source(dataset, 'cad_trip_legs_rev') }} as rev
            ON leg.id = rev.leg_id AND leg.rev = rev.rev
        WHERE rev.deleted = 0
    ),
{% endfor %}

combined as (
    {% for dataset in datasets %}
    {% set suffix=dataset.split('_')[1] %}
    select * from {{ suffix }}_timestamps
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select * from combined