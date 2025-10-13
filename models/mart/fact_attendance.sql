{{ config(materialized = 'table') }}

with primary_services as (
    select
        d.time_bucket,
        m.contact_id,

        -- proportion of worship services attended in person
        safe_divide(
            count(distinct case when e.type = 'Worship Service'
                                and ed.attendance_type = 'In-Person'
                           then e.event_date end),
            count(distinct case when e.type = 'Worship Service'
                           then e.event_date end)
        ) as in_person_prop,

        -- proportion of worship services attended at 11:30 AM
        safe_divide(
            count(distinct case when e.type = 'Worship Service'
                                and e.event_time = '11:30 AM'
                           then e.event_date end),
            count(distinct case when e.type = 'Worship Service'
                           then e.event_date end)
        ) as second_service_prop

    from {{ ref('event_detail') }} ed
    inner join {{ ref('dim_members') }} m
            on ed.contact_id = m.contact_id
    inner join {{ ref('dim_events') }} e
            on ed.event_id = e.event_id
    inner join {{ ref('dim_dates') }} d
            on e.event_date = d.date
    where ed.attendance_indicator = true
    group by 1, 2
)

select distinct
    e.event_id,
    e.event_date,
    d.time_bucket,
    e.event_time,
    e.type,
    e.event_format,
    ed.attendance_type,
    m.contact_id,
    m.first_name,
    m.last_name,
    m.participant_type,
    p.in_person_prop,
    p.second_service_prop,
    count(distinct e.event_date) over (partition by m.contact_id, d.time_bucket) as events_attended,
    count(distinct e.event_date) over (partition by d.time_bucket) as events_sum
from {{ ref('event_detail') }} ed
inner join {{ ref('dim_members') }} m
        on ed.contact_id = m.contact_id
inner join {{ ref('dim_events') }} e
        on ed.event_id = e.event_id
inner join {{ ref('dim_dates') }} d
        on e.event_date = d.date
left join primary_services p
       on p.time_bucket = d.time_bucket
      and p.contact_id  = m.contact_id
where ed.attendance_indicator = true
