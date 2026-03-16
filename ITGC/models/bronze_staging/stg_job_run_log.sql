with source as (
    select * from {{ source('staging', 'BATCH_JOB_RUN') }}
),

renamed as (
    select
        run_id,
        job_name,
        scheduler,
        scheduled_start::timestamp_ntz  as scheduled_start,
        actual_start::timestamp_ntz     as actual_start,
        actual_end::timestamp_ntz       as actual_end,
        duration_mins::int              as duration_mins,
        status,
        exit_code::int                  as exit_code,
        server,
        triggered_by,
        triggered_by_user,
        incident_raised::boolean        as incident_raised,
        incident_id,
        out_of_window::boolean          as out_of_window,
        schedule_changed::boolean       as schedule_changed,
        schedule_change_ticket,
        retry_count::int                as retry_count,
        log_path,
        current_timestamp()             as dbt_loaded_at
    from source
)

select * from renamed