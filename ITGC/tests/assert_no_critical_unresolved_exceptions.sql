-- FAILS if any Critical exception has been open
-- longer than the 5 day remediation SLA
-- This should not happen in a well-controlled environment

select
    test_result_id,
    control_id,
    domain,
    finding_detail,
    days_open,
    remediation_sla_days
from {{ ref('mart_control_exceptions') }}
where severity              = 'Critical'
  and remediation_sla_breached = true