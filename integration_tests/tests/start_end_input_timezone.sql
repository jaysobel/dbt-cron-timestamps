with range_cases as (
  select
    1 as case_id
    , '0 8 * * *' as cron
    , '2024-01-01 00:00:00'::timestamp_ntz as start_at
    , '2024-01-02 00:00:00'::timestamp_ntz as end_at
)

, actual as (
  {{ dbt_cron_timestamps.cron_start_end_to_timestamps(
      'range_cases',
      'cron',
      'start_at',
      'end_at',
      unique_id='case_id',
      max_date_range=1,
      input_timezone='America/Los_Angeles'
  ) }}
)

, expected as (
  select
    1 as case_id
    , '0 8 * * *' as cron
    , '2024-01-01 08:00:00'::timestamp_ntz as trigger_at_utc
)

select * from actual
minus
select * from expected

union all

select * from expected
minus
select * from actual
