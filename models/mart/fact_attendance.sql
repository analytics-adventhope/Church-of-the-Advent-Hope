{{ config(materialized = 'table') }}

select m.member_sk,
       e.event_sk,
       d.date_sk,
       e.event_date,
       e.event_time,
       e.type,
       e.event_format,
       m.participant_type
from {{ ref('event_detail') }} ed
inner join {{ ref('dim_members') }} m
        on cm.contact_id = m.contact_id
inner join {{ ref('dim_events') }} e
        on cm.event_id = e.id
inner join {{ ref('dim_dates') }} d
        on e.event_date = d.date
where cm.attendance_indicator = true;
