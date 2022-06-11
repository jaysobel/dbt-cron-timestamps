with crons as (
    select '5-29/2,31-59/4 */3 4/5 * TUE-WED,1-3' as cron
        union all
    select '1-5/2,6-10/2,59 6,7,8,9-11/1 */2 8 0,1,2,3' as cron 
        union all
    select '15 23 1 9 2' as cron 
        union all
    select '*/59 0-23/1 1-31/4 7-7/2 WED-FRI/2' as cron 
)

, cron_timestamps as (
    {{ cron_to_timestamps('crons', 'cron', 'current_date', days_forward=60, day_match_mode='contains') }}
)

select *
from cron_timestamps