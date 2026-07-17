with crons as (
  select '0 0 * * *' as cron
)

, actual as (
  {{ dbt_cron_timestamps.cron_to_timestamps(
      'crons',
      'cron',
      "dateadd('day', -10, current_date)",
      days_forward=1,
      day_match_mode='vixie'
  ) }}
)

select *
from actual
where trigger_at_utc != dateadd('day', -10, current_date)::timestamp_ntz

union all

select 'wrong row count', null::timestamp_ntz
from actual
having count(*) != 1
