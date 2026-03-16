{{
    config(
        materialized         = 'incremental',
        unique_key           = 'test_result_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

with change_ticket as (
    select * from {{ ref('stg_change_tickets') }}
    {% if is_incremental() %}
        where created_at > (
            select coalesce(max_ts, '1970-01-01'::timestamp_ntz)
            from (
                select max(event_timestamp) as max_ts
                from {{ this }}
            )
        )
    {% endif %}
),

deployment as (
    select * from {{ ref('stg_deployment_log') }}
    {% if is_incremental() %}
        where deployment_ts > (
            select coalesce(max_ts, '1970-01-01'::timestamp_ntz)
            from (
                select max(event_timestamp) as max_ts
                from {{ this }}
            )
        )
    {% endif %}
),

cm1_tests as (
    select
        d.deployment_id,
        d.system_name,
        d.deployment_ts,
        'CM-1'                                          as control_id,
        'Change Management'                             as domain,
        'Deployment associated with change ticket'      as control_name,
        case
            when d.change_ticket_id is null             then 'FAIL'
            when ct.status != 'APPROVED'                then 'FAIL'
            else                                             'PASS'
        end                                             as test_result,
        case
            when d.change_ticket_id is null             then 'High'
            when ct.status in ('PENDING','REJECTED')    then 'High'
            when ct.status = 'IN_REVIEW'                then 'Medium'
            else                                             null
        end                                             as severity,
        case
            when d.change_ticket_id is null
            then 'Production deployment with no change ticket linked'
            when ct.status in ('PENDING','REJECTED','CANCELLED')
            then 'Linked ticket not approved — status: ' || ct.status
            when ct.status = 'IN_REVIEW'
            then 'Deployment made while change ticket still under review'
            else 'Control passed'
        end                                             as finding_detail
    from deployment d
    left join change_ticket ct
        on d.change_ticket_id = ct.ticket_id
    qualify row_number() over (
        partition by d.deployment_id, 'CM-1'
        order by case when ct.status = 'APPROVED' then 1 else 0 end desc
    ) = 1
),

cm2_tests as (
    select
        d.deployment_id,
        d.system_name,
        d.deployment_ts,
        'CM-2'                                          as control_id,
        'Change Management'                             as domain,
        'UAT completion before production deploy'       as control_name,
        case
            when ct.uat_completed = false
             and ct.uat_sign_off_by is null             then 'FAIL'
            when ct.uat_completed = true
             and ct.uat_sign_off_by is null             then 'WARN'
            else                                             'PASS'
        end                                             as test_result,
        case
            when ct.uat_completed = false
             and ct.uat_sign_off_by is null             then 'High'
            when ct.uat_completed = true
             and ct.uat_sign_off_by is null             then 'Medium'
            else                                             null
        end                                             as severity,
        case
            when ct.uat_completed = false
             and ct.uat_sign_off_by is null
            then 'UAT not completed before production deployment'
            when ct.uat_completed = true
             and ct.uat_sign_off_by is null
            then 'UAT completed but no sign-off recorded'
            else 'Control passed'
        end                                             as finding_detail
    from deployment d
    left join change_ticket ct
        on d.change_ticket_id = ct.ticket_id
    where d.environment = 'PROD'
      and ct.status     = 'APPROVED'
    qualify row_number() over (
        partition by d.deployment_id
        order by ct.created_at desc
    ) = 1
),

deployment_gaps as (
    select
        deployment_id,
        system_name,
        deployment_ts,
        datediff('hour',
            lag(deployment_ts) over (
                partition by system_name, environment
                order by deployment_ts
            ),
            deployment_ts
        )                                               as hours_since_last_deploy
    from deployment
    where environment = 'PROD'
),

cm3_tests as (
    select
        deployment_id,
        system_name,
        deployment_ts,
        'CM-3'                                          as control_id,
        'Change Management'                             as domain,
        'Unusual deployment frequency detected'         as control_name,
        case
            when hours_since_last_deploy is null        then 'PASS'
            when hours_since_last_deploy < 6            then 'FAIL'
            when hours_since_last_deploy < 24           then 'WARN'
            else                                             'PASS'
        end                                             as test_result,
        case
            when hours_since_last_deploy < 6            then 'High'
            when hours_since_last_deploy < 24           then 'Medium'
            else                                             null
        end                                             as severity,
        case
            when hours_since_last_deploy is null
            then 'First deployment for this system — no prior baseline'
            when hours_since_last_deploy < 6
            then 'Suspicious burst: deployed only '
                  || hours_since_last_deploy
                  || ' hours after previous deployment'
            when hours_since_last_deploy < 24
            then 'Elevated frequency: '
                  || hours_since_last_deploy
                  || ' hours since last deployment'
            else 'Normal deployment frequency'
        end                                             as finding_detail
    from deployment_gaps
    where hours_since_last_deploy is null
       or hours_since_last_deploy < 24
    qualify row_number() over (
        partition by deployment_id
        order by deployment_ts desc
    ) = 1
),

final as (
    select * from cm1_tests
    union all
    select * from cm2_tests
    union all
    select * from cm3_tests
)

select
    md5(
        cast(control_id    as varchar) || '-' ||
        cast(deployment_id as varchar)
    )                                                   as test_result_id,
    control_id,
    domain,
    control_name,
    deployment_id                                       as event_key,
    system_name                                         as entity_name,
    deployment_ts                                       as event_timestamp,
    test_result,
    severity,
    finding_detail,
    current_timestamp()                                 as tested_at
from final