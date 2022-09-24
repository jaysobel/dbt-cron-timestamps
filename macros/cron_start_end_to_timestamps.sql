{% macro cron_start_end_to_timestamps(
     cte_name
     , cron_column_name
     , start_at_column_name
     , end_at_column_name
     , max_date_range='1095'
     , day_match_mode='vixie'
   ) 
%}

{#-
  A macro to generate timestamps from cron expression strings and start/end timestamps. 
  There are multiple flavors of cron. This macro is based on the information provided at crontab.guru.
  Timestamps are generated additively from matched time-parts, rather than reductively from all possible matches. 
  This is a relatively efficient approach, but result sets can still be large.
  The output of this macro populates a CTE with SQL resulting in two columns: `cron`, `cron_range_sk` and `trigger_at_utc`. 
  Results are distinct.

  :param cte_name: The name of a preceding CTE containing cron expressions.
  :param cron_column_name: The name of the column in `cte_name` that contains the cron expressions.
  :param start_at_column_name: The name of the column in `cte_name` that contains the generation range start as a timestamp or date.
  :param start_at_column_name: The name of the column in `cte_name` that contains the generation range end as a timestamp or date.
  :param max_date_range: The maximum number of days between an entrys start and end columns. Default is 1095 (365*3).
  :param day_match_mode: Default 'vixie'. Alternatively `contains`, `union` or `intersect`. This parameter controls how day 
    matching is performed. The day_of_month and day_of_week parts are either unioned or intersected. In some implementations
    of cron, this is based on the presence of an * in the first position of each entry (vixie). Others expand this 'first entry'
    to any position of the entry (contains). See [this write up](https://crontab.guru/cron-bug.html) for details.
    Other implementations use exclusively intersect or union, which can be coded here.

  :return: A SQL select statement of ~250 lines that culminates in a distinct selection of: `cron, cron_range_sk, trigger_at_utc`.

  ## Example call ##:
  ```
  with some_cron_cte as (
    select id, cron_code, created_at, next_created_at, other_column from {{ ref('some_other_model') }}
  )
  , cron_timestamps as (
    {{ cron_start_end_to_timestamps('some_cron_cte', 'cron_code', 'created_at', 'next_created_at') }}
  )
  select 
    some_cron_cte.cron_code
    , cron_timestamps.trigger_at_utc
  
  from some_cron_cte
  inner join cron_timestamps 
    on concat(some_cron_cte.cron_code, '-', date(created_at), '-', date(next_created_at)) = cron_timestamps.cron_range_sk
  ```
-#}

  with cron_day_match_mode as (
    select distinct 
      {{ cron_column_name }} as cron
      -- In case these are timestamps
      , {{ start_at_column_name }} as start_raw
      , date({{ start_at_column_name }}) as start_date
      , {{ end_at_column_name }} as end_raw
      , date({{ end_at_column_name }}) as end_date
      , concat(
          {{ cron_column_name }}
          , '-', date({{ start_at_column_name }})
          , '-', date({{ end_at_column_name }})
        ) as cron_range_sk

      -- Safe to ignore
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
    where {{ start_at_column_name }} < {{ end_at_column_name }}
  )

  , max_days_forward as (
    select max(datediff('day', start_date, end_date)) + 1 as days_forward
    from cron_day_match_mode
  )

  , numbers as (
    select row_number() over (order by 1) - 1 as num
    from table (generator(rowcount => number({{max_date_range}}) )) -- the maximum start/end range allowable
  )

  -- fan each cron by it's start-end date range
  , cron_dates as (
    select
      crons.cron_range_sk
      , crons.cron
      , crons.start_raw
      , crons.end_raw
      , dateadd('day', numbers.num, crons.start_date) as date
      , crons.day_match_mode
    from cron_day_match_mode as crons
    inner join numbers 
      on datediff('day', crons.start_date, crons.end_date) + 1 >= numbers.num
      and numbers.num between 0 and (select days_forward from max_days_forward)
  )

  -- set up the range of values for each time part
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
    from cron_dates
    
      union all
    
    select distinct
      'month' as cron_part
      , month(date) as value
      , right(concat('0', value::text), 2) as value_text
    from cron_dates
    
      union all
    
    select distinct
      'day_of_week' as cron_part
      , dayofweek(date) as value
      , value::text as value_text
    from cron_dates
  )

  -- deafults for replacing asterisk wildcard 
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

  -- Fan cron codes by space separator, then by comma separator, extract range (2-4) and step sizes (/3)
  -- Also convert asterisk to full range, and text entries (day-of-week, month names) to numerics.
  , cron_part_comma_subentries as (
    select 
      crons.cron_range_sk
      , crons.cron
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
      and comma_numbers.num between 1 and 10 -- maximum comma-separated subentries to split within a part
  )

  -- Join time part ranges against time part entries on betweeness and mod of step size.
  , cron_part_matched_values as (
    select
      cpcs.cron_range_sk
      , cpcs.cron
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
    group by 1,2,3,4,5,6
  )

  -- Join cron day ranges with matched months, and both day types, dependent on day_match_mode
  , cron_dates_matched as (
    select 
      cron_dates.cron_range_sk
      , cron_dates.cron
      , cron_dates.start_raw
      , cron_dates.end_raw
      , cron_dates.date

    from cron_dates
    -- filter months
    inner join cron_part_matched_values as cron_part_month
      on cron_dates.cron_range_sk = cron_part_month.cron_range_sk
      and month(cron_dates.date) = cron_part_month.value
      and cron_part_month.cron_part = 'month'
    -- align matched dates, and filter based on each cron's day_match_mode
    left join cron_part_matched_values as cron_part_day_of_month
      on cron_dates.cron_range_sk = cron_part_day_of_month.cron_range_sk
      and dayofmonth(cron_dates.date) = cron_part_day_of_month.value
      and cron_part_day_of_month.cron_part = 'day_of_month'
    left join cron_part_matched_values as cron_part_day_of_week
      on cron_dates.cron_range_sk = cron_part_day_of_week.cron_range_sk
      and dayofweek(cron_dates.date) = cron_part_day_of_week.value
      and cron_part_day_of_week.cron_part = 'day_of_week'
    where 
      (
        (cron_dates.day_match_mode = 'union' 
        and (cron_part_day_of_month.value is not null 
              or cron_part_day_of_week.value is not null))
        or 
        (cron_dates.day_match_mode = 'intersect' 
        and cron_part_day_of_month.value is not null
        and cron_part_day_of_week.value is not null)
      )
  )

  -- Fan matched dates to matched hours and minutes, and construct timestamps.
  select 
    cron_dates_matched.cron_range_sk
    , cron_dates_matched.cron
    , to_timestamp_ntz(
        concat(
          cron_dates_matched.date
          , ' '
          , cron_part_hour.value_text, ':', cron_part_minute.value_text
        )
      ) as trigger_at_utc

  from cron_dates_matched
  -- fan to hours
  inner join cron_part_matched_values as cron_part_hour
    on cron_dates_matched.cron_range_sk = cron_part_hour.cron_range_sk
    and cron_part_hour.cron_part = 'hour'
  -- fan to minutes
  inner join cron_part_matched_values as cron_part_minute
    on cron_dates_matched.cron_range_sk = cron_part_minute.cron_range_sk
    and cron_part_minute.cron_part = 'minute'

  -- filter between precise start/end timestamps
  where trigger_at_utc between cron_dates_matched.start_raw and cron_dates_matched.end_raw

{% endmacro %}
