with days as (select date('2020-01-01') + interval x DAY as date
              from unnest(generate_array(0, 18250)) as x  -- 50 years
             )
select distinct format_date('%Y%m%d', date) as date_sk,
       date,
       extract(dayofweek from date) as day_of_week,
       extract(week from date) as week,
       extract(month from date) as month,
       extract(quarter from date) as quarter,
       extract(year from date) as year
from days
