with source as (
    select * from {{ source('staging', 'DEPLOYMENT') }}
),

renamed as (
    select
        deployment_id,
        system_name,
        environment,
        deployed_by,
        deployment_ts::timestamp_ntz    as deployment_ts,
        change_ticket_id,
        branch_name,
        deployment_method,
        duration_mins::int              as duration_mins,
        status,
        is_emergency::boolean           as is_emergency,
        notes,
        current_timestamp()             as dbt_loaded_at
    from source
)

select * from renamed