-- FAILS if any expected control ID is missing
-- from the results — ensures no control test
-- silently stopped producing output

select expected.control_id
from (
    select 'AC-1' as control_id union all
    select 'AC-2' union all
    select 'AC-3' union all
    select 'CM-1' union all
    select 'CM-2' union all
    select 'CM-3' union all
    select 'OPS-1' union all
    select 'OPS-2' union all
    select 'BR-1' union all
    select 'BR-2'
) expected
left join (
    select distinct control_id
    from {{ ref('mart_itgc_control_results') }}
) actual on expected.control_id = actual.control_id
where actual.control_id is null