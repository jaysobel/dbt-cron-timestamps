{{ config(materialized='ephemeral') }}

with crons as (
  select cron
  from {{ ref('cron_cases') }}
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

select *
from actual
