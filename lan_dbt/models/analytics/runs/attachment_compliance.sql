{{ config(materialized='table') }}

/*
    Attachment Compliance Analytics

    One row per CREW MEMBER per RUN - enables flexible aggregation.

    Attribution Rules:
    - If the finalize_user is a crew member on the run: only that person gets counted
    - If the finalize_user is NOT a crew member (e.g., admin): all crew members get counted
    - Compliance is based on whether ANY crew member uploaded attachments (team effort)

    Compliance Rules:
    - Only 'ran' runs count (not 'turned' or 'cancelled')
    - Dialysis and Doctors Appointment runs do NOT require attachments
    - All other completed runs require attachments from crew members

    Key Metrics (aggregate in PowerBI):
    - Compliance rate: COUNT(is_compliant=true) / COUNT(requires_attachment=true)
*/

{% set datasets=['traumasoft_tn', 'traumasoft_mi', 'traumasoft_il'] %}

WITH
runs AS (
    SELECT * FROM {{ ref('stg_runs') }}
),

run_timestamps AS (
    SELECT * FROM {{ ref('stg_run_timestamps') }}
),

run_cancels AS (
    SELECT * FROM {{ ref('stg_run_cancels') }}
),

attachment_log AS (
    SELECT * FROM {{ ref('stg_attachment_log') }}
),

-- Get finalization info from epcr_v2_runs (deduplicated - one per run_number)
{% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
{{ suffix }}_epcr_runs AS (
    SELECT DISTINCT ON (return_run_num)
        return_run_num AS run_number,
        finalize_user,
        finalized,
        '{{ suffix }}' AS source_database
    FROM {{ source(dataset, 'epcr_v2_runs') }}
    WHERE return_run_num IS NOT NULL
    ORDER BY return_run_num, finalized DESC NULLS LAST, id DESC
){% if not loop.last %},{% endif %}
{% endfor %},

all_epcr_runs AS (
    SELECT * FROM tn_epcr_runs
    UNION ALL
    SELECT * FROM mi_epcr_runs
    UNION ALL
    SELECT * FROM il_epcr_runs
),

-- Get crew assignments for each run (user_id per leg via shift assignments)
{% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
{{ suffix }}_leg_assignments AS (
    SELECT
        la.leg_id,
        la.shift_assignment_id,
        s.user_id,
        '{{ suffix }}' AS source_database
    FROM {{ source(dataset, 'cad_trip_leg_shift_assignments') }} la
    INNER JOIN {{ source(dataset, 'sched_template_shift_assignments') }} s
        ON la.shift_assignment_id = s.id
    WHERE s.user_id IS NOT NULL
){% if not loop.last %},{% endif %}
{% endfor %},

all_leg_crew AS (
    SELECT * FROM tn_leg_assignments
    UNION ALL
    SELECT * FROM mi_leg_assignments
    UNION ALL
    SELECT * FROM il_leg_assignments
),

-- Get user names for display
{% for dataset in datasets %}
{% set suffix=dataset.split('_')[1] %}
{{ suffix }}_users AS (
    SELECT
        user_id,
        first_name,
        last_name,
        CONCAT(last_name, ', ', first_name) AS full_name,
        employee_num,
        '{{ suffix }}' AS source_database
    FROM {{ source(dataset, 'users') }}
){% if not loop.last %},{% endif %}
{% endfor %},

all_users AS (
    SELECT * FROM tn_users
    UNION ALL
    SELECT * FROM mi_users
    UNION ALL
    SELECT * FROM il_users
),

-- Attachments with crew member flag
attachments_with_crew_flag AS (
    SELECT
        al.log_id,
        al.attachment_id,
        al.user_id AS uploader_user_id,
        al.action_timestamp,
        al.file_name,
        al.leg_id,
        al.source_database,
        al.attachment_types,
        CASE
            WHEN lc.user_id IS NOT NULL THEN true
            ELSE false
        END AS uploaded_by_crew_member
    FROM attachment_log al
    LEFT JOIN all_leg_crew lc
        ON al.leg_id = lc.leg_id
        AND al.source_database = lc.source_database
        AND al.user_id = lc.user_id
),

-- Aggregate attachments per run
run_attachment_summary AS (
    SELECT
        leg_id,
        source_database,
        COUNT(*) AS total_attachments,
        COUNT(*) FILTER (WHERE uploaded_by_crew_member) AS crew_attachments,
        STRING_AGG(DISTINCT attachment_types, '; ' ORDER BY attachment_types) AS all_attachment_types,
        MIN(action_timestamp) AS first_attachment_time
    FROM attachments_with_crew_flag
    GROUP BY leg_id, source_database
),

-- Base run data with proper run_outcome calculation
run_base AS (
    SELECT
        r.run_number,
        r.leg_id,
        r.pcr_number,
        r.source_database,
        r.last_status_id,
        CASE
            WHEN r.source_database = 'il' THEN 'il'
            WHEN r.source_database = 'mi' THEN 'mi'
            WHEN r.source_database = 'tn' AND r.market IN ('Memphis', 'Mississippi', 'Event - MEMP', 'Event - MISS') THEN 'tn_memphis'
            WHEN r.source_database = 'tn' AND r.market IN ('Nashville', 'Event - NASH') THEN 'tn_nashville'
            ELSE r.source_database
        END AS region,
        -- Run outcome (same logic as bq_runs)
        CASE
            WHEN r.last_status_id > 0 THEN 'ran'
            WHEN r.last_status_id < 0 AND c.lost_call_status IS NOT NULL THEN 'turned'
            WHEN r.last_status_id < 0 AND c.lost_call_status IS NULL THEN 'cancelled'
            WHEN r.last_status_id IS NULL THEN 'cancelled'
            ELSE 'unknown'
        END AS run_outcome,
        rt.service_date,
        DATE_TRUNC('week', rt.service_date)::date AS week_start,
        DATE_TRUNC('month', rt.service_date)::date AS month_start,
        r.calltype_name,
        r.level_of_service,
        r.market,
        r.source_name,
        r.reason_for_transport,
        -- Finalization info
        epcr.finalize_user,
        epcr.finalized,
        -- Attachment summary
        COALESCE(ras.total_attachments, 0) AS total_attachments,
        COALESCE(ras.crew_attachments, 0) AS crew_attachments,
        ras.all_attachment_types,
        ras.first_attachment_time,
        CASE WHEN ras.crew_attachments > 0 THEN true ELSE false END AS has_crew_attachment,
        CASE
            WHEN r.reason_for_transport IN ('Dialysis', 'Doctors Appointment') or r.source_name like '%Vet Ride%' or r.calltype_name not in ('ALS','BLS','CCT') THEN false
            ELSE true
        END AS requires_attachment,
        CASE
            WHEN r.reason_for_transport IN ('Dialysis', 'Doctors Appointment') or r.source_name like '%Vet Ride%' or r.calltype_name not in ('ALS','BLS','CCT') THEN  true
            WHEN ras.crew_attachments > 0 THEN true
            ELSE false
        END AS is_compliant
    FROM runs r
    INNER JOIN run_timestamps rt
        ON r.leg_id = rt.leg_id
        AND r.source_database = rt.source_database
    LEFT JOIN run_cancels c
        ON r.leg_id = c.leg_id
        AND r.source_database = c.source_database
    LEFT JOIN all_epcr_runs epcr
        ON cast(r.run_number as varchar) = epcr.run_number
        AND r.source_database = epcr.source_database
    LEFT JOIN run_attachment_summary ras
        ON r.leg_id = ras.leg_id
        AND r.source_database = ras.source_database
),

-- Filter to only 'ran' runs
ran_runs AS (
    SELECT * FROM run_base
    WHERE run_outcome = 'ran'
),

-- Determine if finalize_user is a crew member on each run
run_finalizer_is_crew AS (
    SELECT DISTINCT
        rb.leg_id,
        rb.source_database,
        rb.finalize_user,
        CASE
            WHEN lc.user_id IS NOT NULL THEN true
            ELSE false
        END AS finalizer_is_crew_member
    FROM ran_runs rb
    LEFT JOIN all_leg_crew lc
        ON rb.leg_id = lc.leg_id
        AND rb.source_database = lc.source_database
        AND rb.finalize_user = lc.user_id
),

-- Final: one row per crew member per run, filtered by attribution rules
crew_run_compliance AS (
    SELECT
        -- Crew member info
        lc.user_id,
        u.full_name AS crew_member_name,
        u.employee_num,
        lc.shift_assignment_id,

        -- Run info
        rb.run_number,
        rb.leg_id,
        rb.pcr_number,
        rb.source_database,
        rb.region,
        rb.run_outcome,
        rb.service_date,
        rb.week_start,
        rb.month_start,
        rb.calltype_name,
        rb.level_of_service,
        rb.market,
        rb.source_name,
        rb.reason_for_transport,

        -- Finalization info
        rb.finalize_user,
        rb.finalized,
        rfc.finalizer_is_crew_member,
        CASE WHEN lc.user_id = rb.finalize_user THEN true ELSE false END AS is_finalizer,

        -- Attachment info
        rb.total_attachments,
        rb.crew_attachments,
        rb.all_attachment_types,
        rb.first_attachment_time,
        rb.has_crew_attachment,
        rb.requires_attachment,
        rb.is_compliant

    FROM all_leg_crew lc
    INNER JOIN ran_runs rb
        ON lc.leg_id = rb.leg_id
        AND lc.source_database = rb.source_database
    LEFT JOIN all_users u
        ON lc.user_id = u.user_id
        AND lc.source_database = u.source_database
    LEFT JOIN run_finalizer_is_crew rfc
        ON rb.leg_id = rfc.leg_id
        AND rb.source_database = rfc.source_database
    WHERE
        -- Attribution rule: include this crew member if:
        -- 1. They are the finalizer, OR
        -- 2. The finalizer is not a crew member on this run (admin finalized)
        lc.user_id = rb.finalize_user
        OR rfc.finalizer_is_crew_member = false
        OR rb.finalize_user IS NULL  -- Run not finalized yet, count all crew
)

SELECT * FROM crew_run_compliance
ORDER BY service_date DESC, run_number, crew_member_name