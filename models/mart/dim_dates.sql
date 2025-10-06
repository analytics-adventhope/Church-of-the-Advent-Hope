with days as (
    select date('2020-01-01') + interval x day as date
    from unnest(generate_array(0, 18250)) as x  -- ~50 years
),
max_date as (
    select current_date('EST') as max_end_date
)
select distinct date(d.date) as date,
       extract(dayofweek from d.date) as day_of_week,
       extract(week from d.date) as week,
       extract(month from d.date) as month,
       extract(quarter from d.date) as quarter,
       extract(year from d.date) as year,
       case when date_diff(m.max_end_date, d.date, month) <= 3 then '3-month'
            when date_diff(m.max_end_date, d.date, month) <= 6 then '6-month'
            when date_diff(m.max_end_date, d.date, month) <= 12 then '12-month'
            when date_diff(m.max_end_date, d.date, month) <= 24 then '24-month'
            else 'all time'
       end as time_bucket
from days d
cross join max_date m
where d.date <= m.max_end_date
order by 1 desc
