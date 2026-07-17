with actual as (
  select cron, trigger_at_utc
  from {{ ref('cron_actual') }}
)

, expected as (
  select cron, trigger_at_utc
  from {{ ref('cron_expected') }}
)

, unexpected as (
  select * from actual
  minus
  select * from expected
)

, missing as (
  select * from expected
  minus
  select * from actual
)

select 'unexpected' as difference, *
from unexpected

union all

select 'missing' as difference, *
from missing
