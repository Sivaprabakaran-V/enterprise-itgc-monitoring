-- FAILS if any event_timestamp is in the future
-- Catches data quality issues in source systems
-- or incorrect timestamp casting

select
    test_result_id,
    control_id,
    event_timestamp
from {{ ref('mart_itgc_control_results') }}
where event_timestamp > current_timestamp()