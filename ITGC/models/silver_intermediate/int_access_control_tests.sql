{{
    config(
        materialized         = 'incremental',
        unique_key           = 'test_result_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

with provisioning as (
    select * from {{ ref('stg_access_provisioning_log') }}

    {% if is_incremental() %}
        where event_timestamp > (
            select max(event_timestamp) from {{ this }}
        )
    {% endif %}
),

hr as (
    select * from {{ ref('stg_hr_events') }}

    {% if is_incremental() %}
        where event_date > (
            select dateadd('day', -7, max(event_timestamp))
            from {{ this }}
        )
    {% endif %}
),

privileged_register as (
        select * from {{ ref('stg_privileged_access_register') }}
),

-- ── AC-1: checking the access provisioning without approved requests
ac1_tests as (
    select
        log_id                                          as event_key,
        employee_id,
        system_name,
        action,
        role_granted,
        event_timestamp,
        'AC-1'                                          as control_id,
        'Access Management'                             as domain,
        'Provisioning without approved request'         as control_name,
        case
            when request_ticket_id is null
             and approved_by is null                   then 'FAIL'   -- in an access controlled environment, both a ticket and an approver should be required for provisioning
            when request_ticket_id is null
              or approved_by is null                   then 'WARN'  -- while ideally both a ticket and approver would be required, if one is present it may indicate that at least some level of control was in place, so we flag this as a warning
            else                                            'PASS' -- if both a ticket and approver are present, we consider this a pass, though in practice you may want to consider other factors such as the content of the ticket or the identity of the approver
        end                                             as test_result,
        case
            when request_ticket_id is null
             and approved_by is null                   then 'High'
            when request_ticket_id is null
              or approved_by is null                   then 'Medium'
            else                                            null
        end                                             as severity,
        case
            when request_ticket_id is null
             and approved_by is null
            then 'No ticket and no approver recorded'
            when request_ticket_id is null
            then 'No request ticket linked'
            when approved_by is null
            then 'No approver recorded'
            else 'Control passed'
        end                                             as finding_detail
    from provisioning
    where action = 'GRANT'
),

-- ── AC-2: checking for active access after employee termination
terminations as (
    select
        employee_id,
        event_date                                      as termination_date
    from hr
    where event_type = 'TERMINATION'
),

post_termination_access as (
    select
        p.log_id                                        as event_key,
        p.employee_id,
        p.system_name,
        p.action,
        p.role_granted,
        p.event_timestamp,
        t.termination_date,
        datediff('hour',
            t.termination_date,
            p.event_timestamp)                          as hours_after_termination
    from provisioning      p
    inner join terminations t
        on  p.employee_id = t.employee_id
    where p.event_timestamp > t.termination_date
      and p.action in ('GRANT','ENABLE','MODIFY')
),

ac2_tests as (
    select
        event_key,
        employee_id,
        system_name,
        action,
        role_granted,
        event_timestamp,
        'AC-2' as control_id,
        'Access Management' as domain,
        'Active access after employee termination' as control_name,
        case
            when hours_after_termination > 168 then 'FAIL'
            when hours_after_termination > 24 then 'WARN'
            else 'FAIL'
        end as test_result,
        case
            when hours_after_termination > 168 then 'Critical'
            when hours_after_termination > 24 then 'High'
            else 'High'
        end as severity,
        'Access event ' || hours_after_termination
            || ' hours after termination' as finding_detail
    from post_termination_access
),

ac3_tests as (
    select
        p.log_id as event_key,
        p.employee_id,
        p.system_name,
        p.action,
        p.role_granted,
        p.event_timestamp,
        'AC-3' as control_id,
        'Access Management' as domain,
        'Unauthorized privileged role granted' as control_name,
        case
            when r.employee_id is null then 'FAIL'
            else 'PASS'
        end as test_result,
        case
            when r.employee_id is null then 'Critical'
            else null
        end as severity,
        case
            when r.employee_id is null
                then 'User not on privileged access register for role: '
                     || p.role_granted
            else 'Authorized - found on privileged register'
        end as finding_detail
    from provisioning p
    left join privileged_register r
        on p.employee_id = r.employee_id
        and p.role_granted = r.privileged_role
        and p.system_name = r.system_name
        and r.is_active = true
    where p.role_granted in (
        'SYSADMIN','DBA','SECURITY_ADMIN',
        'BACKUP_ADMIN','NETWORK_ADMIN','DOMAIN_ADMIN'
    )
),

final as (
    select * from ac1_tests
    union all
    select * from ac2_tests
    union all
    select * from ac3_tests
)

select
    md5(control_id || '-' || event_key) as test_result_id,
    control_id,
    domain,
    control_name,
    event_key,
    employee_id             as entity_name,
    event_timestamp,
    test_result,
    severity,
    finding_detail,
    current_timestamp()     as tested_at
from final