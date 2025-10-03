{{ config(materialized = 'table') }}

select m.contact_id,
       e.event_id,
       d.date_sk,
       e.event_date,
       e.event_time,
       e.type,
       e.event_format,
       m.participant_type
from {{ ref('event_detail') }} ed
inner join {{ ref('dim_members') }} m
        on ed.contact_id = m.contact_id
inner join {{ ref('dim_events') }} e
        on ed.event_id = e.id
inner join {{ ref('dim_dates') }} d
        on e.event_date = d.date
where ed.attendance_indicator = true
