with source as (
    select * from {{ source('staging', 'RESTORATION_TEST') }}
),

renamed as (
    select
        test_id,
        system_name,
        test_date::timestamp_ntz        as test_date,
        test_type,
        conducted_by,
        approved_by,
        rto_target_hrs::float           as rto_target_hrs,
        rto_actual_hrs::float           as rto_actual_hrs,
        rto_met::boolean                as rto_met,
        rpo_target_hrs::float           as rpo_target_hrs,
        rpo_actual_hrs::float           as rpo_actual_hrs,
        rpo_met::boolean                as rpo_met,
        overall_result,
        issues_found::int               as issues_found,
        remediation_plan::boolean       as remediation_plan,
        it_mgr_sign_off::boolean        as it_mgr_sign_off,
        next_test_due::timestamp_ntz    as next_test_due,
        notes,
        current_timestamp()             as dbt_loaded_at
    from source
)

select * from renamed