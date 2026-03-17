{{ config(materialized='table') }}

/*
    BigQuery Export: Attachment Compliance

    Exports attachment compliance data for PowerBI reporting.
    One row per crew member per run (where they are responsible for compliance).
*/

SELECT
    -- Crew member
    user_id,
    crew_member_name,
    employee_num,

    -- Run identifiers
    run_number,
    leg_id,
    source_database,
    region,

    -- Time dimensions
    service_date,
    week_start,
    month_start,

    -- Run details
    calltype_name,
    level_of_service,
    market,
    source_name,
    reason_for_transport,

    -- Finalization
    finalize_user,
    finalized,
    finalizer_is_crew_member,
    is_finalizer,

    -- Compliance
    total_attachments,
    crew_attachments,
    all_attachment_types,
    has_crew_attachment,
    requires_attachment,
    is_compliant

FROM {{ ref('attachment_compliance') }}