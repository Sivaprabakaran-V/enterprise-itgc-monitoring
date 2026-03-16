-- FAILS if severity is populated for PASS rows
-- or missing for FAIL rows
-- Validates control logic consistency

select
    test_result_id,
    control_id,
    test_result,
    severity
from {{ ref('mart_itgc_control_results') }}
where
    -- PASS rows should never have a severity
    (test_result = 'PASS' and severity is not null)
    or
    -- FAIL rows must always have a severity
    (test_result = 'FAIL' and severity is null)