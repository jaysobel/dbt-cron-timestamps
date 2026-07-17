with range_cases as (
  select
    column1::int as case_id
    , iff(
        column1 = 3,
        concat('  17   6', char(9), '*   *', char(9), 'MON  '),
        column2
      )::varchar as cron
    , column3::timestamp_ntz as start_at_utc
    , column4::timestamp_ntz as end_at_utc
  from values
    (1, '41 11 * * 1-7', '2024-02-01 00:00:00', '2024-02-05 23:59:00'),
    (2, '1 15 * * 1/2', '2024-03-01 12:00:00', '2024-03-08 15:01:00'),
    (3, '', '2024-04-01 06:17:00', '2024-04-08 06:17:00'),
    (4, '4 18 */32,1-7 * WED', '2024-05-01 00:00:00', '2024-05-10 00:00:00')
)

, actual as (
  {{ dbt_cron_timestamps.cron_start_end_to_timestamps(
      'range_cases',
      'cron',
      'start_at_utc',
      'end_at_utc',
      unique_id='case_id',
      max_date_range='10',
      day_match_mode='vixie'
  ) }}
)

, expected as (
  select
    range_cases.case_id
    , range_cases.cron
    , cron_expected.trigger_at_utc
  from range_cases
  inner join {{ ref('cron_expected') }} as cron_expected
    on range_cases.cron = cron_expected.cron
    and cron_expected.trigger_at_utc >= range_cases.start_at_utc
    and cron_expected.trigger_at_utc < range_cases.end_at_utc
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
