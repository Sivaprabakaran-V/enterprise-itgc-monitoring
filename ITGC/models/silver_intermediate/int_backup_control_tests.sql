{{
    config(
        materialized         = 'incremental',
        unique_key           = 'test_result_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

with backups as (
    select * from {{ ref('stg_backup_job_log') }}

    {% if is_incremental() %}
        where start_time > (
            select coalesce(max_ts, '1970-01-01'::timestamp_ntz)
            from (
                select max(event_timestamp) as max_ts
                from {{ this }}
                where control_id = 'BR-1'
            )
        )
    {% endif %}
),

restoration_tests as (
    select * from {{ ref('stg_restoration_test_log') }}

    {% if is_incremental() %}
        where test_date > (
            select coalesce(max_ts, '1970-01-01'::timestamp_ntz)
            from (
                select max(event_timestamp) as max_ts
                from {{ this }}
                where control_id = 'BR-2'
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
                where control_id = 'BR-1'
            )
        )
    {% endif %}
),

failed_backups as (
    select *
    from backups
    where status in ('FAILED', 'PARTIAL')
),

br1_tests as (
    select
        b.backup_id,
        b.system_name,
        b.backup_tool,
        b.backup_type,
        b.start_time,
        b.status,
        b.is_critical_system,
        b.incident_raised,
        i.incident_id                                       as linked_incident_id,
        i.sla_breached                                      as incident_sla_breached,
        'BR-1'                                              as control_id,
        'Backup and Recovery'                               as domain,
        'Failed backup with no incident raised'             as control_name,
        case
            when b.incident_raised = FALSE                  then 'FAIL'
            when b.incident_raised = TRUE
             and i.sla_breached    = TRUE                   then 'WARN'
            else                                                 'PASS'
        end                                                 as test_result,
        case
            when b.is_critical_system = TRUE
             and b.incident_raised    = FALSE               then 'Critical'
            when b.is_critical_system = FALSE
             and b.incident_raised    = FALSE               then 'High'
            when b.incident_raised    = TRUE
             and i.sla_breached       = TRUE                then 'Medium'
            else                                                 null
        end                                                 as severity,
        case
            when b.incident_raised = FALSE
             and b.is_critical_system = TRUE
            then 'Critical backup failure — no incident raised for: '
                  || b.system_name
            when b.incident_raised = FALSE
            then 'Backup failed with no incident raised for: '
                  || b.system_name
            when b.incident_raised = TRUE
             and i.sla_breached    = TRUE
            then 'Incident raised but SLA breached for backup on: '
                  || b.system_name
            else 'Control passed'
        end                                                 as finding_detail
    from failed_backups b
    left join incidents i
        on b.incident_id = i.incident_id
),

all_systems as (
    select distinct
        system_name,
        is_critical_system
    from backups
),

recent_tests as (
    select
        system_name,
        max(test_date)                                            as last_test_date,
        max(case when overall_result = 'PASS' then 1 else 0 end) as had_passing_test
    from restoration_tests
    where test_date > dateadd('month', -12, current_date())
    group by system_name
),

br2_tests as (
    select
        b.system_name,
        b.is_critical_system,
        rt.last_test_date,
        rt.had_passing_test,
        'BR-2'                                                  as control_id,
        'Backup and Recovery'                                   as domain,
        'No successful restoration test in past 12 months'     as control_name,
        case
            when rt.last_test_date is null                      then 'FAIL'
            when rt.had_passing_test = 0                        then 'WARN'
            else                                                     'PASS'
        end                                                     as test_result,
        case
            when b.is_critical_system = TRUE
             and rt.last_test_date    is null                   then 'Critical'
            when b.is_critical_system = FALSE
             and rt.last_test_date    is null                   then 'High'
            when rt.had_passing_test  = 0                       then 'Medium'
            else                                                     null
        end                                                     as severity,
        case
            when rt.last_test_date is null
            then 'No restoration test in past 12 months for: '
                  || b.system_name
            when rt.had_passing_test = 0
            then 'Restoration test exists but no passing result for: '
                  || b.system_name
            else 'Control passed'
        end                                                     as finding_detail
    from all_systems b
    left join recent_tests rt
        on b.system_name = rt.system_name
),

final as (
    select
        backup_id                       as event_key,
        system_name                     as entity_name,
        start_time                      as event_timestamp,
        control_id,
        domain,
        control_name,
        test_result,
        severity,
        finding_detail
    from br1_tests

    union all

    select
        system_name                     as event_key,
        system_name                     as entity_name,
        current_date()::timestamp_ntz   as event_timestamp,
        control_id,
        domain,
        control_name,
        test_result,
        severity,
        finding_detail
    from br2_tests
)

select
    md5(
        cast(control_id as varchar) || '-' ||
        cast(event_key  as varchar)
    )                                   as test_result_id,
    control_id,
    domain,
    control_name,
    event_key,
    entity_name,
    event_timestamp,
    test_result,
    severity,
    finding_detail,
    current_timestamp()                 as tested_at
from final