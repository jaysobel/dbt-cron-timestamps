with crons as (
  select cron
  from {{ ref('cron_stress') }}
)

, actual as (
  {{ dbt_cron_timestamps.cron_to_timestamps(
      'crons',
      'cron',
      '2024-01-01',
      days_forward=366,
      day_match_mode='vixie'
  ) }}
)

, actual_aggregates as (
  select
    cron
    , count(*) as actual_count
    , md5(
        listagg(to_char(trigger_at_utc, 'YYYY-MM-DD HH24:MI:SS'), '|')
          within group (order by trigger_at_utc)
      ) as actual_md5
  from actual
  group by cron
)

select
  expected.cron
  , expected.expected_count
  , coalesce(actual.actual_count, 0) as actual_count
  , expected.expected_md5
  , coalesce(actual.actual_md5, md5('')) as actual_md5
from {{ ref('cron_stress') }} as expected
left join actual_aggregates as actual
  on expected.cron = actual.cron
where expected.expected_count != coalesce(actual.actual_count, 0)
  or expected.expected_md5 != coalesce(actual.actual_md5, md5(''))
