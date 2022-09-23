{% macro cron_to_timestamps(
     cte_name='crons'
     , cron_column_name='cron'
     , start_date='current_date'
     , days_forward=365
     , day_match_mode='vixie'
   ) 
%}

{#-
  A macro to generate timestamps from cron expression strings. Timestamps are limited by a passed 
  `start_date` and `days_forward` integer. 
  There are multiple flavors of cron. This macro is based on the information provided at crontab.guru.
  Timestamps are generated additively from matched time-parts, rather than reductively from all possible matches. 
  This is a relatively efficient approach, but result sets can still be large. Use the `start_date` 
  and `days_forward` parameters to limit results accordingly.
  The output of this macro populates a CTE with SQL resulting in two columns: `cron` and `trigger_at_utc`. 
  Results are distinct.

  :param cte_name: The name of a preceding CTE containing cron expressions.
  :param cron_column_name: The name of the column in `cte_name` that contains the cron expressions.
  :param start_date: The starting date from which to produce timestamps. Invoked with: `date({{ start_date }})`.
  :param days_forward: The number of days forward from the `start_date` within which to generate matching timestamps.
  :param day_match_mode: Default 'vixie'. Alternatively `contains`, `union` or `intersect`. This parameter controls how day 
    matching is done. In some implementations of cron, the presence of an asterisk in either the `day_of_month` or `day_of_week` 
    positions determines whether day matches are unioned or intersected. In `vixie` mode, only the first character is checked, 
    see [this write up](https://crontab.guru/cron-bug.html) for details. Passing `contains` will check all positions, 
    while passing `union` or `intersect` will hard-code the behavior, as some modern implementations use `intersect` by default.

  :return: A SQL select statement of ~200 lines that culminates in a two column distinct selection: `cron, trigger_at_utc`.

  ## Example call ##:
  ```
  with some_cron_cte as (
    select id, cron_code, other_column from {{ ref('some_other_model') }}
  )

  , cron_timestamps as (
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
-#}

  with cron_day_match_mode as (
    select distinct 
      {{ cron_column_name }} as cron

      {% if day_match_mode == 'vixie' -%}
      
      , case
          when not left(split_part({{ cron_column_name }}, ' ', 3), 1) = '*'
          and not left(split_part({{ cron_column_name }}, ' ', 5), 1) = '*'
          then 'union'
          else 'intersect'
        end as day_match_mode
      
      {%- elif day_match_mode == 'contains' -%}

      , case
          when not left(split_part({{ cron_column_name }}, ' ', 3), 1) like '%*%'
          and not left(split_part({{ cron_column_name }}, ' ', 5), 1) like '%*%'
          then 'union'
          else 'intersect'
        end as day_match_mode

      {%- elif day_match_mode in ('union', 'intersect') -%}

      , '{{ day_match_mode }}' as day_match_mode

      {%- endif %}

    from {{ cte_name }}
  )

  , dates_in_range as (
    
    {% if '0' in start_date  -%} 
    select dateadd('day', row_number() over (order by 1) - 1, date('{{ start_date }}')) as date
    {%- else -%}
    select dateadd('day', row_number() over (order by 1) - 1, date({{ start_date }})) as date
    {%- endif %}

    from table (generator(rowcount => {{ days_forward }} ))
  )

  , numbers as (
    select row_number() over (order by 1) - 1 as num
    from table (generator(rowcount => 60))
  )

  , cron_part_values as (
    select 
      'minute' as cron_part
      , num as value
      , right(concat('0', value::text), 2) as value_text
    from numbers
    where num between 0 and 59
    
      union all 

    select 
      'hour' as cron_part
      , num as value
      , right(concat('0', value::text), 2) as value_text
    from numbers
    where num between 0 and 23
    
      union all
    
    select distinct
      'day_of_month' as cron_part
      , dayofmonth(date) as value
      , right(concat('0', value::text), 2) as value_text
    from dates_in_range
    
      union all
    
    select distinct
      'month' as cron_part
      , month(date) as value
      , right(concat('0', value::text), 2) as value_text
    from dates_in_range
    
      union all
    
    select distinct
      'day_of_week' as cron_part
      , dayofweek(date) as value
      , value::text as value_text
    from dates_in_range

      union all

    -- not a real cron part, but used like the others later
    select distinct
      'year' as cron_part
      , year(date) as value
      , value::text as value_text
    from dates_in_range
  )

  , cron_part_defaults as (
    select
      num as part_number
      , case num
          when 1 then 'minute'
          when 2 then 'hour'
          when 3 then 'day_of_month'
          when 4 then 'month'
          when 5 then 'day_of_week'
        end as cron_part
      , case num
          when 1 then '0-59'
          when 2 then '0-23'
          when 3 then '1-31'
          when 4 then '1-12'
          when 5 then '0-6'
        end star_range
    from numbers
    where num between 1 and 5 
  )

  , cron_part_comma_subentries as (
    select 
      crons.cron
      , case space_number.num
          when 1 then 'minute'
          when 2 then 'hour'
          when 3 then 'day_of_month'
          when 4 then 'month'
          when 5 then 'day_of_week'
        end as cron_part
        -- replace asterisk with equivalent full-range selector (per cron_part)
      , replace(split_part(crons.cron, ' ', space_number.num), '*', cron_part_defaults.star_range) as cron_part_entry_raw
      , case 
          when space_number.num = 5
          then 
            replace(replace(replace(replace(
              replace(replace(replace(replace(
                upper(cron_part_entry_raw), '7', '0'), 'SUN', '0'), 'MON', '1'), 'TUE', '2')
              , 'WED', '3'), 'THU', '4'), 'FRI', '5'), 'SAT', '6')
          when space_number.num = 4
          then 
            replace(replace(replace(replace(replace(replace(
              replace(replace(replace(replace(replace(replace(
                upper(cron_part_entry_raw), 'JAN', '1'), 'FEB', '2'), 'MAR', '3'), 'APR', '4'), 'MAY', '5'), 'JUN', '6')
              , 'JUL', '7'), 'AUG', '8'), 'SEP', '9'), 'OCT', '10'), 'NOV', '11'), 'DEC', '12')
          else cron_part_entry_raw
        end as cron_part_entry
      , comma_numbers.num as cron_part_entry_comma_index
      , split_part(cron_part_entry, ',', cron_part_entry_comma_index) as cron_part_comma_subentry
      , split_part(cron_part_comma_subentry, '/', 1) as cron_part_comma_subentry_range
      , coalesce(nullif(split_part(cron_part_comma_subentry, '/', 2), '')::int, 1) as cron_part_comma_subentry_step_value
      , split_part(cron_part_comma_subentry_range, '-', 1)::int as cron_part_comma_subentry_range_start
      , coalesce(
          nullif(split_part(cron_part_comma_subentry_range, '-', 2), '')::int
          -- if a step size is provided, fill with range max, otherwise fill with range start (for between)
          , case 
              when regexp_count(cron_part_comma_subentry, '/') = 1
              then split_part(cron_part_defaults.star_range, '-', 2)::int
              else cron_part_comma_subentry_range_start
            end
        ) as cron_part_comma_subentry_range_end
    from cron_day_match_mode as crons
    inner join numbers as space_number
      on space_number.num between 1 and 5
    inner join cron_part_defaults
      on space_number.num = cron_part_defaults.part_number
    inner join numbers as comma_numbers
      on regexp_count(split_part(crons.cron, ' ', space_number.num), ',') + 1 >= comma_numbers.num
      and comma_numbers.num between 1 and 50 -- maximum commas to split
  )

  , cron_part_matched_values as (
    select
      cpcs.cron
      , cpcs.cron_part
      , cpcs.cron_part_entry
      , cpv.value
      , cpv.value_text
        -- Can capture a list of matching sub-entries here, thanks SQL!
      , listagg(cpcs.cron_part_comma_subentry, ', ') within group (order by cpcs.cron_part_entry_comma_index asc) as matching_subentries_list
    from cron_part_comma_subentries as cpcs
    inner join cron_part_values as cpv
      on cpcs.cron_part = cpv.cron_part
      and cpv.value between cpcs.cron_part_comma_subentry_range_start and cpcs.cron_part_comma_subentry_range_end
      and mod(cpv.value - cpcs.cron_part_comma_subentry_range_start, cpcs.cron_part_comma_subentry_step_value) = 0
    group by 1,2,3,4,5
  )

  select 
    crons.cron
    , to_timestamp_ntz(
        concat(
          years.value_text, '-', cron_part_month.value_text, '-', month_days.value_text
          , ' '
          , cron_part_hour.value_text, ':', cron_part_minute.value_text
        )
      ) as trigger_at_utc

  from cron_day_match_mode as crons
  inner join cron_part_values as years
    on years.cron_part = 'year'
  inner join cron_part_matched_values as cron_part_minute
    on crons.cron = cron_part_minute.cron
    and cron_part_minute.cron_part = 'minute'
  inner join cron_part_matched_values as cron_part_hour
    on crons.cron = cron_part_hour.cron
    and cron_part_hour.cron_part = 'hour'
  inner join cron_part_matched_values as cron_part_month
    on crons.cron = cron_part_month.cron
    and cron_part_month.cron_part = 'month'
  -- fan to all days of month
  inner join cron_part_values as month_days
    on month_days.cron_part = 'day_of_month'
    -- remove days beyond last actual day of month: Feb 30th
    and month_days.value <= dayofmonth(last_day(to_date(concat(years.value_text, '-', cron_part_month.value_text, '-', '01'))))
  -- left matched day_of_month
  left join cron_part_matched_values as cron_part_day_of_month
    on crons.cron = cron_part_day_of_month.cron
    and month_days.value = cron_part_day_of_month.value
    and cron_part_day_of_month.cron_part = 'day_of_month'
  -- left matched day_of_week
  left join cron_part_matched_values as cron_part_day_of_week
    on crons.cron = cron_part_day_of_week.cron
    -- Snowflake can get ahead of itself and attempt to construct this date before the last_day() check...
    and dayofweek(try_to_date(concat(years.value_text, '-', cron_part_month.value_text, '-', month_days.value_text))) = cron_part_day_of_week.value
    and cron_part_day_of_week.cron_part = 'day_of_week'

  {% if '0' in start_date  -%} 
  where trigger_at_utc between date('{{ start_date }}') and dateadd('day', {{ days_forward }}, date('{{ start_date }}'))
  {%- else -%}
  where trigger_at_utc between date({{ start_date }}) and dateadd('day', {{ days_forward }}, date({{ start_date }}))
  {%- endif %}
    and (
      (crons.day_match_mode = 'union' 
      and (cron_part_day_of_month.value is not null 
            or cron_part_day_of_week.value is not null))
      or 
      (crons.day_match_mode = 'intersect' 
      and cron_part_day_of_month.value is not null
      and cron_part_day_of_week.value is not null)
    )

{% endmacro %}
