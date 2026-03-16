with source as (
    select * from {{ source('staging', 'BACKUP_JOB') }}
),

renamed as (
    select
        backup_id,
        system_name,
        backup_tool,
        backup_type,
        start_time::timestamp_ntz       as start_time,
        end_time::timestamp_ntz         as end_time,
        duration_mins::int              as duration_mins,
        status,
        size_gb::float                  as size_gb,
        offsite_replicated::boolean     as offsite_replicated,
        offsite_location,
        encrypted::boolean              as encrypted,
        retention_days::int             as retention_days,
        incident_raised::boolean        as incident_raised,
        incident_id,
        is_critical_system::boolean     as is_critical_system,
        verified::boolean               as verified,
        operator_id,
        current_timestamp()             as dbt_loaded_at
    from source
)

select * from renamed