with source as (
    select * from {{ source('staging', 'PRIVILEGED_ACCESS_REGISTER') }}
),

renamed as (
    select
        register_id,
        employee_id,
        employee_name,
        system_name,
        privileged_role,
        approved_by,
        approval_date::timestamp_ntz    as approval_date,
        review_due_date::timestamp_ntz  as review_due_date,
        is_active::boolean              as is_active,
        notes,
        current_timestamp()             as dbt_loaded_at
    from source
)

select * from renamed