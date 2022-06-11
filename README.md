### dbt Cron to Timestamps package.

[Cron](https://en.wikipedia.org/wiki/Cron) expressions are a syntax for pattern-matching timestamps.

This macro fans out a table of one-or-more cron expression by their matching timestamps within a range of dates.

## Usage 

This macro is used as the sole entry in a CTE, and it interacts with another CTE that you have already defined, and that contains cron expressions.
As parameters, the model takes the name of the preceding CTE, and the name of the column containing cron expressions. In this example, `some_cron_cte`
and `cron_code` are the values passed to the macro. It will reference `some_cron_cte.cron_code` in its compiled SQL.

The macro also takes a date-like string for the start date (it runs `date(<start_date_string>)`) and a number of days forward in which
to generate matching timestamps. Cron is a pattern-matching expression, and requires this start and end to output finite matches.

## Example Usage
  ```
  with some_cron_cte as (
    select 
      id
      , cron_code
      , other_column 
    from {{ ref('some_other_model') }}
  )

  -- Grain: cron | timestamp 
  , cron_timestamps as (
    -- invoking the macro
    {{ cron_to_timestamps('some_cron_cte', 'cron_code', 'current_date', 60) }}
  )

  select 
    some_cron_cte.cron_code
    , cron_timestamps.trigger_at_utc
  
  from some_cron_cte
  inner join cron_timestamps 
    on some_cron_cte.cron_code = cron_timestamps.cron

  where cron_timestamps.trigger_at_utc > current_timestamp
  ```

## Additiona Considerations

Cron, like SQL, comes in many flavors. The time-parts `day-of-month` and `day-of-week` overlap in their consideration of days. Various implementations of
cron treat these two sets of matches differently. The classic implementation covered in crontab.guru](https://crontab.guru/) will `intersect` the matched days 
from each part, but only if one or both contain a `*`, and only if the `*` is in the first position. See the [cron bug](https://crontab.guru/cron-bug.html) article
for deeper reference. If neither day part starts with `*`, the results are combined as a `union`, meaning that a matched day need only match one of the day part expressions.

The first CTE of the macro determines the "day match mode", and an optional parameter `day_match_mode` can be set to `vixie` (default), `contains` (check beyond first position),
`intersect` or `union` to force a parcticular strategy.

