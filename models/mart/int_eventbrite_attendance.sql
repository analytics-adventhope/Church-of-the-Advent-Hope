{{ config(materialized='view') }}

-- Normalizes Eventbrite attendance to align with event_detail schema.
-- Grain: one row per attendee per event (deduplicated).
-- Resolves Salesforce contact_id via email; unmatched contacts get an eb_-prefixed ID.

with sf_email_to_contact as (
    -- When multiple SF contacts share an email, take the one with the most recent event attendance
    select
        lower(trim(email))                                                        as email,
        array_agg(contact_id order by event_date desc limit 1)[offset(0)]        as contact_id
    from {{ ref('event_detail') }}
    where email is not null
    group by 1
),

deduped_attendees as (
    -- Some registrants have multiple rows for the same event (duplicate registrations).
    -- Prefer the checked_in = TRUE row; otherwise take the earliest attendee_id.
    select * except (rn)
    from (
        select *,
               row_number() over (
                   partition by eventbrite_event_id, lower(trim(email))
                   order by checked_in desc, eventbrite_attendee_id asc
               ) as rn
        from {{ ref('stg_eventbrite_attendees') }}
    )
    where rn = 1
)

select
    e.eventbrite_event_id                                                         as event_id,
    e.event_name,
    e.type,
    e.event_format,
    e.event_date,
    e.event_time,
    coalesce(sf.contact_id, concat('eb_', a.eventbrite_attendee_id))              as contact_id,
    a.first_name,
    a.last_name,
    lower(trim(a.email))                                                           as email,
    a.phone_raw                                                                    as phone,
    a.checked_in                                                                   as attendance_indicator,
    case when e.event_format = 'Online' then 'Online' else 'In-Person' end         as attendance_type
from deduped_attendees a
inner join {{ ref('stg_eventbrite_events') }} e
        on a.eventbrite_event_id = e.eventbrite_event_id
left join sf_email_to_contact sf
       on lower(trim(a.email)) = sf.email
