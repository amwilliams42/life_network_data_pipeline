{{ config(materialized='view') }}

{%- set dataset_configs = [
  {'dataset': 'traumasoft_tn', 'suffix': 'tn'},
  {'dataset': 'traumasoft_mi', 'suffix': 'mi'},
  {'dataset': 'traumasoft_il', 'suffix': 'il'}
] -%}

with
    {%- for config in dataset_configs %}
        {{ config.suffix }}_cancelled as (
            SELECT
                leg.id as leg_id,
                leg.run_number,
                
                -- Cancellation timing
                rev.canceled_time,
                rev.last_status_timestamp as time_canceled,
                
                -- Cancellation details
                rev.canceled_reason as cancel_reason_text,
                rev.canceled_reason_id,
                cancel_reason.name as cancel_reason_name,
                
                -- Lost call status
                rev.lost_call_status,
                
                -- Who cancelled it
                CONCAT(users.last_name, ', ', users.first_name) as canceled_by,
                canceled_log.user_id as canceled_by_user_id,
                
                -- Source database
                '{{ config.suffix }}' as source_database

            FROM {{ source(config.dataset, 'cad_trip_legs') }} as leg
                INNER JOIN {{ source(config.dataset, 'cad_trip_legs_rev') }} as rev 
                    ON rev.leg_id = leg.id AND rev.rev = leg.rev
                LEFT JOIN {{ source(config.dataset, 'cad_trip_cancel_reason') }} as cancel_reason 
                    ON cancel_reason.cancel_reason_id = rev.canceled_reason_id
                LEFT JOIN (
                    SELECT 
                        leg_id,
                        user_id,
                        ROW_NUMBER() OVER (PARTITION BY leg_id ORDER BY id DESC) as rn
                    FROM {{ source(config.dataset, 'cad_trip_history_log') }}
                    WHERE field = 'canceled_time'
                ) as canceled_log ON canceled_log.leg_id = leg.id AND canceled_log.rn = 1
                LEFT JOIN {{ source(config.dataset, 'users') }} as users 
                    ON users.user_id = canceled_log.user_id
            
            WHERE 
                -- Only cancelled runs
                rev.last_status_id < 0
                -- Filter out deleted records
                AND rev.deleted = 0
        ) {{ "," if not loop.last }}
{%- endfor %}

{%- for config in dataset_configs %}
select * from {{ config.suffix }}_cancelled
{%- if not loop.last %}
union all
{% endif %}
{%- endfor %}