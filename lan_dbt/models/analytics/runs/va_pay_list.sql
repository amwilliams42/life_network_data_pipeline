SELECT
             DISTINCT leg.run_number,
                      rev.leg_date,
                      rev.clear_time,
                      rev.assigned_time,
                      EXTRACT(EPOCH FROM (rev.clear_time - rev.assigned_time))/3600 AS time_on_task,
                      cad_sources.name AS source,
                      rev.ordering_facility_name,
                      CONCAT(users.first_name, ' ', users.last_name) AS name,
                      users.hourly_wage
         FROM
             {{ source('traumasoft_tn', 'cad_trip_legs') }} AS leg
                 INNER JOIN {{ source('traumasoft_tn', 'cad_trip_legs_rev') }} AS rev ON rev.leg_id = leg.id
                 AND rev.rev = leg.rev
                 INNER JOIN {{ source('traumasoft_tn', 'epcr_v2_cad_legs') }} AS epcr_leg ON epcr_leg.cad_leg_id = leg.id
                 INNER JOIN {{ source('traumasoft_tn', 'cad_trips') }} AS c_trip ON c_trip.id = leg.trip_id
                 LEFT JOIN {{ source('traumasoft_tn', 'cad_sources') }} ON cad_sources.id = rev.source_id
                 LEFT JOIN {{ source('traumasoft_tn', 'cad_trip_leg_shift_assignments') }} AS filter_crew_sa ON filter_crew_sa.leg_id = leg.id
                 LEFT JOIN {{ source('traumasoft_tn', 'sched_template_shift_assignments') }} AS filter_crew_stsa ON filter_crew_stsa.id = filter_crew_sa.shift_assignment_id
                 LEFT JOIN {{ source('traumasoft_tn', 'sched_shifts') }} AS shift ON shift.id = filter_crew_stsa.shift_id
                 LEFT JOIN {{ source('traumasoft_tn', 'users') }} on users.user_id = filter_crew_stsa.user_id
         WHERE (
             rev.deleted = '0'
             )
           AND (
             rev.source_id IN (12,13)
             )
           AND (
               rev.last_status_id >= -1
             )
         ORDER BY
             1