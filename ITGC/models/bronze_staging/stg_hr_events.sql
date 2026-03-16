WITH source as (
    select * from {{ source('staging', 'HR_EVENTS') }}

),

renamed as 
(
    SELECT
       event_id, 
       employee_id,
       employee_name,
       department,
       event_type,
       event_date,
       performed_by,
       notes,
       current_timestamp() as dbt_loaded_at

    FROM source
)

SELECT * FROM renamed