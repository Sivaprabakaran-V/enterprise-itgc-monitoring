{{
    config(
        materialized         = 'incremental',
        unique_key           = 'test_result_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

with all_results as (
    select * from {{ ref('mart_itgc_control_results') }}
    {% if is_incremental() %}
        where tested_at > (
            select coalesce(max_ts, '1970-01-01'::timestamp_ntz)
            from (
                select max(tested_at) as max_ts
                from {{ this }}
            )
        )
    {% endif %}
),

exceptions as (
    select
        test_result_id,
        control_id,
        domain,
        control_name,
        event_key,
        entity_name,
        event_timestamp,
        test_result,
        severity,
        severity_rank,
        finding_detail,
        tested_at,
        -- days open since exception was detected
        datediff('day',
            event_timestamp,
            current_timestamp()
        )                                   as days_open,
        -- remediation SLA based on severity
        case
            when severity = 'Critical'      then 5
            when severity = 'High'          then 15
            when severity = 'Medium'        then 30
            else                                 60
        end                                 as remediation_sla_days,
        -- is this exception breaching remediation SLA?
        case
            when datediff('day',
                event_timestamp,
                current_timestamp()) >
                case
                    when severity = 'Critical'  then 5
                    when severity = 'High'      then 15
                    when severity = 'Medium'    then 30
                    else                             60
                end                         then TRUE
            else                                 FALSE
        end                                 as remediation_sla_breached,
        current_timestamp()                 as mart_loaded_at
    from all_results
    where test_result in ('FAIL', 'WARN')   -- exceptions only
)

select * from exceptions