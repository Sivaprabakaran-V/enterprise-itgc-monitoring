with source as (
    select * from {{ source('staging', 'CHANGE_TICKETS') }}
),

renamed as (
    select
        ticket_id,
        title,
        change_type,
        system_name,
        environment,
        requestor_id,
        created_at::timestamp_ntz       as created_at,
        approved_by,
        approved_at::timestamp_ntz      as approved_at,
        cab_approved::boolean           as cab_approved,
        uat_completed::boolean          as uat_completed,
        uat_sign_off_by,
        status,
        rollback_plan::boolean          as rollback_plan,
        priority,
        closed_at::timestamp_ntz        as closed_at,
        current_timestamp()             as dbt_loaded_at
    from source
)

select * from renamed