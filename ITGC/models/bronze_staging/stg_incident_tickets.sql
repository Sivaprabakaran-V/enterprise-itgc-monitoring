with source as (
    select * from {{ source('staging', 'INCIDENT_TICKETS') }}
),

renamed as (
    select
        incident_id,
        related_job_run_id,
        title,
        priority,
        category,
        created_at::timestamp_ntz       as created_at,
        assigned_to,
        resolved_at::timestamp_ntz      as resolved_at,
        resolution_hrs::float           as resolution_hrs,
        sla_hrs::int                    as sla_hrs,
        sla_breached::boolean           as sla_breached,
        rca_documented::boolean         as rca_documented,
        recurrence_count::int           as recurrence_count,
        status,
        current_timestamp()             as dbt_loaded_at
    from source
)

select * from renamed