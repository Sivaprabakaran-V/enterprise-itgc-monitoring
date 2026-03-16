with source as (
    select * from {{ source('staging', 'ACCESS_PROVISIONING') }}
),

renamed as (
    select
        log_id,
        employee_id,
        employee_name,
        system_name,
        action,
        role_granted,
        event_timestamp::timestamp_ntz  as event_timestamp,
        performed_by,
        approved_by,
        request_ticket_id,
        is_emergency::boolean           as is_emergency,
        source_ip,
        current_timestamp()             as dbt_loaded_at
    from source
)

select * from renamed