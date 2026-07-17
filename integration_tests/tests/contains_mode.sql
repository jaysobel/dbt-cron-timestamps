with crons as (
  select '0 0 1,*/10 * MON' as cron
)

, actual as (
  {{ dbt_cron_timestamps.cron_to_timestamps(
      'crons',
      'cron',
      '2024-01-01',
      days_forward=366,
      day_match_mode='contains'
  ) }}
)

, dates as (
  select dateadd('day', row_number() over (order by 1) - 1, '2024-01-01'::date) as date
  from table(generator(rowcount => 366))
)

, expected as (
  select
    '0 0 1,*/10 * MON' as cron
    , date::timestamp_ntz as trigger_at_utc
  from dates
  where dayofmonth(date) in (1, 11, 21, 31)
    and dayofweekiso(date) = 1
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

select 'unexpected' as difference, * from unexpected
union all
select 'missing' as difference, * from missing
