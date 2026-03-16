{{
    config(
        materialized         = 'incremental',
        unique_key           = 'test_result_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

with access_tests as (
    select * from {{ ref('int_access_control_tests') }}
    {% if is_incremental() %}
        where tested_at > (
            select coalesce(max_ts, '1970-01-01'::timestamp_ntz)
            from (
                select max(tested_at) as max_ts
                from {{ this }}
                where domain = 'Access Management'
            )
        )
    {% endif %}
),

change_tests as (
    select * from {{ ref('int_change_control_tests') }}
    {% if is_incremental() %}
        where tested_at > (
            select coalesce(max_ts, '1970-01-01'::timestamp_ntz)
            from (
                select max(tested_at) as max_ts
                from {{ this }}
                where domain = 'Change Management'
            )
        )
    {% endif %}
),

ops_tests as (
    select * from {{ ref('int_ops_control_tests') }}
    {% if is_incremental() %}
        where tested_at > (
            select coalesce(max_ts, '1970-01-01'::timestamp_ntz)
            from (
                select max(tested_at) as max_ts
                from {{ this }}
                where domain = 'IT Operations'
            )
        )
    {% endif %}
),

backup_tests as (
    select * from {{ ref('int_backup_control_tests') }}
    {% if is_incremental() %}
        where tested_at > (
            select coalesce(max_ts, '1970-01-01'::timestamp_ntz)
            from (
                select max(tested_at) as max_ts
                from {{ this }}
                where domain = 'Backup and Recovery'
            )
        )
    {% endif %}
),

final as (
    select * from access_tests
    union all
    select * from change_tests
    union all
    select * from ops_tests
    union all
    select * from backup_tests
)

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
    finding_detail,
    tested_at,
    -- additional mart columns for dashboard filtering
    case
        when severity = 'Critical'  then 1
        when severity = 'High'      then 2
        when severity = 'Medium'    then 3
        else                             4
    end                             as severity_rank,
    case
        when test_result = 'FAIL'   then 1
        when test_result = 'WARN'   then 2
        else                             3
    end                             as result_rank,
    current_timestamp()             as mart_loaded_at
from final