-- FAILS if any domain drops below 30% pass rate
-- Acts as a sanity check — if pass rate is extremely low
-- it likely means a data or logic issue, not real failures

select
    domain,
    pass_rate_pct
from {{ ref('mart_kri_summary') }}
where summary_level = 'DOMAIN'
  and pass_rate_pct < 30