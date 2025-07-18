{{ config(materialized='view') }}

with tn_users as (
    select
        *,
        'tn' as source_database
    from {{ source('traumasoft_tn','users') }}
),
mi_users as (
    select
        *,
        'mi' as source_database
    from {{ source('traumasoft_mi','users') }}
),
il_users as (
    select
        *,
        'il' as source_database
    from {{ source('traumasoft_il','users') }}
),

combined as (
    select * from tn_users
    union all
    select * from il_users
    union all
    select * from mi_users
)

select * from combined