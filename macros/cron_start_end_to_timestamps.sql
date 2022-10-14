{% macro cron_start_end_to_timestamps(
     cte_name
     , cron_column_name
     , start_at_column_name
     , end_at_column_name
     , unique_id=none
     , max_date_range='1095'
     , day_match_mode='vixie'
   ) 
%}

{#-
  Macro to convert a row containiner a cron expressions to rows of matching timestamps 
  within the bounds of a start_at and end_at column.
  takes reference to a CTE with the columns: cron, start_at, end_at and a unique_id
  used to map the output matched timestamps back to their source rows. Start and end can be dates or
  timestamps, ideally UTC. 
  There are multiple flavors of cron. This macro is based on the information provided at crontab.guru.
  Timestamps are generated efficiently, however cron can match up to every minute of time between
  each rows start and end, producing results as large as;
  ```
    select sum(datediff('day', min(start_at), max(end_at))*24*60*60 from your_cte
  ```

  :param cte_name: The name of a preceding CTE containing cron expressions.
  :param cron_column_name: The name of the column in `cte_name` that contains the cron expressions.
  :param start_at_column_name: The name of the column in `cte_name` that contains the generation range start as a timestamp or date.
  :param end_at_column_name: The name of the column in `cte_name` that contains the generation range end as a timestamp or date.
  :param unique_id: The name of the column in `cte_name` that contains a unique identifier.
  :param max_date_range: The maximum number of days between an entrys start and end columns. Default is 1095 (365*3).
  :param day_match_mode: Default vixie. Alternatively `contains`, `union` or `intersect`. This parameter controls how day 
    matching is performed. The day_of_month and day_of_week parts are either unioned or intersected. In some implementations
    of cron, this is based on the presence of an * in the first position of each entry (vixie). Others expand this first entry
    to any position of the entry (contains). See [this write up](https://crontab.guru/cron-bug.html) for details.
    Other implementations use exclusively intersect or union, which can be coded here.

  :return: A SQL select statement of ~300 lines that culminates in a distinct selection of: `cron, cron_date_range_sk, trigger_at_utc`.

  ## Example call ##:
  ```
  with some_cron_cte as (
    select unique_id, cron_code, created_at, next_created_at, other_column from {{ ref('some_other_model') }}
  )
  , cron_timestamps as (
    {{ cron_start_end_to_timestamps('some_cron_cte', 'cron_code', 'created_at', 'next_created_at', 'unique_id') }}
  )
  select 
    some_cron_cte.unique_id
    , cron_timestamps.trigger_at_utc
  
  from some_cron_cte
  inner join cron_timestamps 
    on some_cron_cte.unique_id = cron_timestamps.unique_id
  ```
-#}

  with id_cron_rows as (
    select
      coalesce(nullif({{ unique_id }}::text, ''), {{ cron_column_name }}) as unique_id_{{ unique_id }}
      , {{ cron_column_name }} as cron
      , to_timestamp_ntz(convert_timezone('UTC', {{ start_at_column_name }})) as start_at_utc
      , to_timestamp_ntz(convert_timezone('UTC', {{ end_at_column_name }})) as end_at_utc
    from {{ cte_name }}
    where {{ start_at_column_name }} < {{ end_at_column_name }}
  )
  
  , cron_ranges as (
    select distinct
      cron
      , start_at_utc
      , date(start_at_utc) as start_date
      , end_at_utc
      , date(end_at_utc) as end_date
      -- unique key
      , concat(cron, '-', start_at_utc, '-', end_at_utc) as cron_range_sk

      {% if day_match_mode == 'vixie' -%}
      
      , case
          when not left(split_part(cron, ' ', 3), 1) = '*'
          and not left(split_part(cron, ' ', 5), 1) = '*'
          then 'union'
          else 'intersect'
        end as day_match_mode
      
      {%- elif day_match_mode == 'contains' -%}

      , case
          when not left(split_part(cron, ' ', 3), 1) like '%*%'
          and not left(split_part(cron, ' ', 5), 1) like '%*%'
          then 'union'
          else 'intersect'
        end as day_match_mode

      {%- elif day_match_mode in ('union', 'intersect') -%}

      , '{{ day_match_mode }}' as day_match_mode

      {%- endif %}

    from id_cron_rows
  )

  , numbers as (
    select row_number() over (order by 1) - 1 as num
    from table (generator(rowcount => to_number({{max_date_range}} + 1) )) -- the maximum generated days from start toward end
  )

  -- Fan by ranges by dates, and distinct across cron, start/end, date.
  , cron_range_dates as (
    select distinct
      cron_ranges.cron
      , cron_ranges.day_match_mode
      , cron_ranges.start_date
      , cron_ranges.end_date
      , dateadd('day', numbers.num, cron_ranges.start_date) as date
    from cron_ranges
    inner join numbers 
      on datediff('day', cron_ranges.start_date, cron_ranges.end_date) >= numbers.num
      and numbers.num between 0 and (select max(datediff('day', start_date, end_date)) as days_forward from cron_ranges)
  )

  -- Distinct cron, date.
  , cron_dates as (
    select distinct
      cron
      , day_match_mode
      , date
    from cron_range_dates
  )

  -- Distinct across cron.
  , crons as (
    select distinct
      cron
      , day_match_mode
    from cron_dates
  )

  -- Set up the range of values within each time part
  , part_values as (
    select 
      'minute' as part
      , num as value
      , right(concat('0', value::text), 2) as value_text
    from numbers
    where num between 0 and 59
    
      union all 

    select 
      'hour' as part
      , num as value
      , right(concat('0', value::text), 2) as value_text
    from numbers
    where num between 0 and 23
    
      union all
    
    select distinct
      'day_of_month' as part
      , dayofmonth(date) as value
      , right(concat('0', value::text), 2) as value_text
    from cron_dates
    
      union all
    
    select distinct
      'month' as part
      , month(date) as value
      , right(concat('0', value::text), 2) as value_text
    from cron_dates
    
      union all
    
    select distinct
      'day_of_week' as part
      , dayofweek(date) as value
      , value::text as value_text
    from cron_dates
  )

  -- Set full ranges to replace wildcards "*"
  , part_defaults as (
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

  -- Fan cron codes by space separator, then by comma separator, extract ranges start/end, and step sizes (/3)
  -- Also convert asterisk to full range, and text entries (day-of-week, month names) to numerics.
  , cron_part_subentries as (
    select 
      crons.cron
      , case space_number.num
          when 1 then 'minute'
          when 2 then 'hour'
          when 3 then 'day_of_month'
          when 4 then 'month'
          when 5 then 'day_of_week'
        end as part
        -- replace asterisk with equivalent full-range selector (per cron part)
      , replace(split_part(crons.cron, ' ', space_number.num), '*', part_defaults.star_range) as part_entry_raw
      , case 
          when space_number.num = 5
          then 
            replace(replace(replace(replace(
              replace(replace(replace(replace(
                upper(part_entry_raw), '7', '0'), 'SUN', '0'), 'MON', '1'), 'TUE', '2')
              , 'WED', '3'), 'THU', '4'), 'FRI', '5'), 'SAT', '6')
          when space_number.num = 4
          then 
            replace(replace(replace(replace(replace(replace(
              replace(replace(replace(replace(replace(replace(
                upper(part_entry_raw), 'JAN', '1'), 'FEB', '2'), 'MAR', '3'), 'APR', '4'), 'MAY', '5'), 'JUN', '6')
              , 'JUL', '7'), 'AUG', '8'), 'SEP', '9'), 'OCT', '10'), 'NOV', '11'), 'DEC', '12')
          else part_entry_raw
        end as part_entry
      , comma_numbers.num as part_entry_comma_index
      , split_part(part_entry, ',', part_entry_comma_index) as part_subentry
      , split_part(part_subentry, '/', 1) as part_subentry_range
      , coalesce(nullif(split_part(part_subentry, '/', 2), '')::int, 1) as part_subentry_step_value
      , split_part(part_subentry_range, '-', 1)::int as part_subentry_range_start
      , coalesce(
          nullif(split_part(part_subentry_range, '-', 2), '')::int
          -- if a step size is provided, fill with range max, otherwise fill with range start (for between)
          , case 
              when regexp_count(part_subentry, '/') = 1
              then split_part(part_defaults.star_range, '-', 2)::int
              else part_subentry_range_start
            end
        ) as part_subentry_range_end
      , concat(part, ':', part_subentry_range_start, '-', part_subentry_range_end, '/', part_subentry_step_value) as part_subentry_sk
    from crons
    inner join numbers as space_number
      on space_number.num between 1 and 5
    inner join part_defaults
      on space_number.num = part_defaults.part_number
    inner join numbers as comma_numbers
      on regexp_count(split_part(crons.cron, ' ', space_number.num), ',') >= comma_numbers.num
      and comma_numbers.num between 0 and 10 -- maximum comma-separated subentries to split within a part
  )

  -- Distinct subentries across crons before matching
  , part_subentries as (
    select distinct 
      part
      , part_subentry_sk
      , part_subentry_range_start
      , part_subentry_range_end
      , part_subentry_step_value
    from cron_part_subentries
  )

  -- Join subentries against values on betweeness and mod of step size.
  , part_subentry_values as (
    select distinct
      part_subentries.part
      , part_subentries.part_subentry_sk
      , part_values.value
      , part_values.value_text
    from part_subentries
    inner join part_values
      on part_subentries.part = part_values.part
      and part_values.value between part_subentries.part_subentry_range_start and part_subentries.part_subentry_range_end
      and mod(part_values.value - part_subentries.part_subentry_range_start, part_subentries.part_subentry_step_value) = 0
  )

  -- Fan subentry matches back to crons containing them. Distinct across matched cron, part, value.
  , cron_part_matched_values as (
    select distinct
      cron_part_subentries.cron
      , cron_part_subentries.part
      , part_subentry_values.value
      , part_subentry_values.value_text
    from cron_part_subentries
    inner join part_subentry_values
      on cron_part_subentries.part = part_subentry_values.part
      and cron_part_subentries.part_subentry_sk = part_subentry_values.part_subentry_sk
  )

  -- Filer cron_dates by matched months, both day types w.r.t day_match_mode.
  -- Fan to matched hours and minutes.
  , cron_times_matched as (
    select 
      cron_dates.cron
      , cron_dates.date as trigger_date
      , to_timestamp_ntz(
          concat(
            cron_dates.date
            , ' '
            , cron_part_hour.value_text, ':', cron_part_minute.value_text
          )
        ) as trigger_at_utc

    from cron_dates
    -- filter months
    inner join cron_part_matched_values as cron_part_month
      on cron_dates.cron = cron_part_month.cron
      and month(cron_dates.date) = cron_part_month.value
      and cron_part_month.part = 'month'
    -- align matched dates, and filter based on each cron's day_match_mode
    left join cron_part_matched_values as cron_part_day_of_month
      on cron_dates.cron = cron_part_day_of_month.cron
      and dayofmonth(cron_dates.date) = cron_part_day_of_month.value
      and cron_part_day_of_month.part = 'day_of_month'
    left join cron_part_matched_values as cron_part_day_of_week
      on cron_dates.cron = cron_part_day_of_week.cron
      and dayofweek(cron_dates.date) = cron_part_day_of_week.value
      and cron_part_day_of_week.part = 'day_of_week'
    
    inner join cron_part_matched_values as cron_part_hour
      on cron_dates.cron = cron_part_hour.cron
      and cron_part_hour.part = 'hour'
    inner join cron_part_matched_values as cron_part_minute
      on cron_dates.cron = cron_part_minute.cron
      and cron_part_minute.part = 'minute'

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

  select 
    id_cron_rows.unique_id_{{ unique_id }} as {{ unique_id }} -- hope it's not "cron"!
    , cron_ranges.cron
    , cron_times_matched.trigger_at_utc
    
  from cron_times_matched
  inner join cron_range_dates
    on cron_times_matched.cron = cron_range_dates.cron
    and cron_times_matched.trigger_date = cron_range_dates.date
  inner join cron_ranges
    on cron_times_matched.cron = cron_ranges.cron
    and cron_range_dates.start_date = cron_ranges.start_date
    and cron_range_dates.end_date = cron_ranges.end_date
    and cron_times_matched.trigger_at_utc between cron_ranges.start_at_utc and cron_ranges.end_at_utc
  inner join id_cron_rows
    on cron_ranges.cron = id_cron_rows.cron
    and cron_ranges.start_at_utc = id_cron_rows.start_at_utc
    and cron_ranges.end_at_utc = id_cron_rows.end_at_utc

{% endmacro %}
