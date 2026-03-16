{{
    config(
        materialized='incremental',
        unique_key='test_result_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns')
}}

with job_runs as (
    select * from {{ ref('stg_job_run_log') }}

    {% if is_incremental() %}
        where actual_start > (
            select coalesce(max_ts, '1970-01-01'::timestamp_ntz)
            from (
                select max(event_timestamp) as max_ts
                from {{ this }}
                where control_id = 'OPS-1'
            )
        )
    {% endif %}
),

incidents as (
    select * from {{ ref('stg_incident_tickets') }}

    {% if is_incremental() %}
        where created_at > (
            select coalesce(max_ts, '1970-01-01'::timestamp_ntz)
            from (
                select max(event_timestamp) as max_ts  
                from {{ this }}
                where control_id = 'OPS-2'   
            )
        )
    {% endif %}
),

--- OPS-1: For all failed jobs, check if there is a linked incident ticket. If not, this is a failure. If there is an incident ticket but the SLA was breached, this is a warning, as an incident was raised but the response was not timely. If there is an incident ticket and the SLA was not breached, this is a pass.

failed_jobs as (
    select * 
    from job_runs
    where status in ('FAILED', 'ABENDED')
),

ops1_tests as (

    select 
        j.run_id, 
        j.job_name,
        j.scheduler,
        j.actual_start,
        j.status,
        j.server,
        i.incident_id                                   as linked_incident_id,
        'OPS-1'                                         as control_id,
        'IT Operations'                                 as domain,
        'Job failure with no incident raised'           as control_name,
        case
            when i.incident_id is null then 'FAIL'
            when i.incident_id is not null and 
                i.sla_breached = TRUE then 'WARN'
            else 'PASS'
        end as test_result,
        case
            when i.incident_id is null then 'HIGH'
            when i.sla_breached = TRUE then 'MEDIUM'
            else null
        end as severity,
        case
            when i.incident_id is null then 'job' || j.job_name || ' failed with no linked incident'
            when i.incident_id is not null and 
                i.sla_breached = TRUE then 'Incident raised but SLA breached for job: ' || j.job_name
            else 'Control passed'
        end as finding_detail
    from failed_jobs  j
    left join incidents i
        on j.run_id = i.related_job_run_id
),

---- ── OPS-2: Incident SLA breach by priority
ops2_tests as (
    select 
        incident_id,
        related_job_run_id,
        title,
        priority,
        created_at,
        resolved_at,
        resolution_hrs,
        sla_hrs,
        'OPS-2'                                         as control_id,
        'IT Operations'                                 as domain,
        'Incident SLA breach'                           as control_name,
        case
            when sla_breached = TRUE and priority in ('P1', 'P2') then 'FAIL'
            when sla_breached = TRUE and priority in ('P3', 'P4') then 'WARN'
            else 'PASS'
        end as test_result,
            
           case
            when sla_breached = true
             and priority = 'P1'                       then 'Critical'
            when sla_breached = true
             and priority = 'P2'                       then 'HIGH'
            when sla_breached = true
             and priority in ('P3','P4')               then 'Medium'
            else                                             null
        end as severity,
        case
            when sla_breached = true
            then priority || ' incident breached SLA of '
                  || sla_hrs || ' hours — actual resolution: '
                  || coalesce(cast(resolution_hrs as varchar), 'unresolved')
                  || ' hours'
            else 'Control passed'
        end                                             as finding_detail
    from incidents
),
final as (
    select
        run_id          as event_key,
        job_name        as entity_name,
        actual_start    as event_timestamp,
        control_id,
        domain,
        control_name,
        test_result,
        severity,
        finding_detail
    from ops1_tests

    union all

    select
        incident_id     as event_key,
        title           as entity_name,
        created_at      as event_timestamp,
        control_id,
        domain,
        control_name,
        test_result,
        severity,
        finding_detail
    from ops2_tests
)

select
    md5(
        cast(control_id  as varchar) || '-' ||
        cast(event_key   as varchar)
    )                                                   as test_result_id,
    control_id,
    domain,
    control_name,
    event_key,
    entity_name,
    event_timestamp,
    test_result,
    severity,
    finding_detail,
    current_timestamp()                                 as tested_at
from final
            
           
        