SELECT
             DISTINCT stg.run_number,
                      ts.service_date AS leg_date,
                      ts.clear_time,
                      ts.assigned_time,
                      EXTRACT(EPOCH FROM (ts.clear_time - ts.assigned_time))/3600 AS time_on_task,
                      stg.source_name AS source,
                      rev.ordering_facility_name,
                      CONCAT(users.first_name, ' ', users.last_name) AS name,
                      users.hourly_wage
         FROM
             {{ ref('stg_runs') }} AS stg
                 INNER JOIN {{ ref('stg_run_timestamps') }} AS ts ON ts.leg_id = stg.leg_id AND ts.source_database = stg.source_database
                 INNER JOIN {{ source('traumasoft_tn', 'cad_trip_legs_rev') }} AS rev ON rev.leg_id = stg.leg_id
                 INNER JOIN {{ source('traumasoft_tn', 'epcr_v2_cad_legs') }} AS epcr_leg ON epcr_leg.cad_leg_id = stg.leg_id
                 LEFT JOIN {{ source('traumasoft_tn', 'cad_trip_leg_shift_assignments') }} AS filter_crew_sa ON filter_crew_sa.leg_id = stg.leg_id
                 LEFT JOIN {{ source('traumasoft_tn', 'sched_template_shift_assignments') }} AS filter_crew_stsa ON filter_crew_stsa.id = filter_crew_sa.shift_assignment_id
                 LEFT JOIN {{ source('traumasoft_tn', 'sched_shifts') }} AS shift ON shift.id = filter_crew_stsa.shift_id
                 LEFT JOIN {{ source('traumasoft_tn', 'users') }} on users.user_id = filter_crew_stsa.user_id
         WHERE 
             stg.source_database = 'tn'
           AND stg.source_id IN (12,13)
           AND stg.last_status_id >= -1
           AND users.hourly_wage < 19.36
         ORDER BY
             1