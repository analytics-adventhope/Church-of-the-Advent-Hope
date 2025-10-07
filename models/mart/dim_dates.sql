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
       case when date_diff(m.max_end_date, d.date, month) / 30 <= 3  then 'Months 0 - 3'
            when date_diff(m.max_end_date, d.date, month) / 30 <= 6  then 'Months 4- 6'
            when date_diff(m.max_end_date, d.date, month) / 30 <= 12 then 'Months 7 - 12'
            when date_diff(m.max_end_date, d.date, month) / 30 <= 24 then 'Months 13 - 24'
            else 'Months 25+'
       end as time_bucket
from days d
cross join max_date m
where d.date <= m.max_end_date
order by 1 desc
