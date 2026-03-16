{{
    config(
        materialized = 'table',
        on_schema_change = 'sync_all_columns'
    )
}}

-- Note: KRI summary is materialized as TABLE not incremental
-- It's a full aggregation — always needs to reflect current state
-- Small row count (one row per domain per control) so full refresh is fine

with all_results as (
    select * from {{ ref('mart_itgc_control_results') }}
),

-- ── Domain level summary ───────────────────────────────────
domain_summary as (
    select
        domain,
        'ALL_CONTROLS'                                          as control_id,    -- ← add this
        'All Controls'                                          as control_name,  -- ← add this
        count(*)                                                as total_tests,
        sum(case when test_result = 'PASS' then 1 else 0 end)  as total_passed,
        sum(case when test_result = 'FAIL' then 1 else 0 end)  as total_failed,
        sum(case when test_result = 'WARN' then 1 else 0 end)  as total_warnings,
        sum(case when severity = 'Critical' then 1 else 0 end) as critical_count,
        sum(case when severity = 'High' then 1 else 0 end)     as high_count,
        sum(case when severity = 'Medium' then 1 else 0 end)   as medium_count,
        round(
            sum(case when test_result = 'PASS' then 1 else 0 end)
            * 100.0 / nullif(count(*), 0), 1
        )                                                       as pass_rate_pct,
        max(tested_at)                                          as last_tested_at
    from all_results
    group by domain
),

-- ── Control level summary ──────────────────────────────────
control_summary as (
    select
        domain,
        control_id,
        control_name,
        count(*)                                                as total_tests,
        sum(case when test_result = 'PASS' then 1 else 0 end)  as total_passed,
        sum(case when test_result = 'FAIL' then 1 else 0 end)  as total_failed,
        sum(case when test_result = 'WARN' then 1 else 0 end)  as total_warnings,
        sum(case when severity = 'Critical' then 1 else 0 end) as critical_count,
        round(
            sum(case when test_result = 'PASS' then 1 else 0 end)
            * 100.0 / nullif(count(*), 0), 1
        )                                                       as pass_rate_pct,
        -- overall control health rating
        case
            when sum(case when severity = 'Critical' then 1 else 0 end) > 0
                                                                then 'CRITICAL'
            when sum(case when test_result = 'FAIL' then 1 else 0 end) > 0
                                                                then 'AT RISK'
            when sum(case when test_result = 'WARN' then 1 else 0 end) > 0
                                                                then 'NEEDS ATTENTION'
            else                                                     'HEALTHY'
        end                                                     as control_health,
        max(tested_at)                                          as last_tested_at
    from all_results
    group by domain, control_id, control_name
),

-- ── Overall project summary ────────────────────────────────
overall_summary as (
    select
        'ALL DOMAINS'                                           as domain,
        'OVERALL'                                               as control_id,
        'Overall ITGC Health'                                   as control_name,
        count(*)                                                as total_tests,
        sum(case when test_result = 'PASS' then 1 else 0 end)  as total_passed,
        sum(case when test_result = 'FAIL' then 1 else 0 end)  as total_failed,
        sum(case when test_result = 'WARN' then 1 else 0 end)  as total_warnings,
        sum(case when severity = 'Critical' then 1 else 0 end) as critical_count,
        round(
            sum(case when test_result = 'PASS' then 1 else 0 end)
            * 100.0 / nullif(count(*), 0), 1
        )                                                       as pass_rate_pct,
        case
            when sum(case when severity = 'Critical' then 1 else 0 end) > 0
                                                                then 'CRITICAL'
            when sum(case when test_result = 'FAIL' then 1 else 0 end) > 0
                                                                then 'AT RISK'
            when sum(case when test_result = 'WARN' then 1 else 0 end) > 0
                                                                then 'NEEDS ATTENTION'
            else                                                     'HEALTHY'
        end                                                     as control_health,
        max(tested_at)                                          as last_tested_at
    from all_results
),

final as (
    -- domain level rows
    select
        domain,
        control_id,
        control_name,
        'DOMAIN'                as summary_level,
        total_tests,
        total_passed,
        total_failed,
        total_warnings,
        critical_count,
        high_count,
        medium_count,
        pass_rate_pct,
        null                    as control_health,
        last_tested_at
    from domain_summary

    union all

    -- control level rows
    select
        domain,
        control_id,
        control_name,
        'CONTROL'               as summary_level,
        total_tests,
        total_passed,
        total_failed,
        total_warnings,
        critical_count,
        0                       as high_count,
        0                       as medium_count,
        pass_rate_pct,
        control_health,
        last_tested_at
    from control_summary

    union all

    -- overall row
    select
        domain,
        control_id,
        control_name,
        'OVERALL'               as summary_level,
        total_tests,
        total_passed,
        total_failed,
        total_warnings,
        critical_count,
        0                       as high_count,
        0                       as medium_count,
        pass_rate_pct,
        control_health,
        last_tested_at
    from overall_summary
)
select
    *,
    current_timestamp() as mart_loaded_at
from final