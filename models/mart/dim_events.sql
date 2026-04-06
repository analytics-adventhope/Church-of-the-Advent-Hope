-- Salesforce events
select distinct event_id,
       event_name,
       description,
       type,
       event_format,
       event_date,
       event_time,
       budget,
       cost,
       expected_revenue
from {{ ref('event_detail') }}

union all

-- Eventbrite events (event_ids are numeric strings; no collision with Salesforce 18-char IDs)
select distinct
       eventbrite_event_id       as event_id,
       event_name,
       description,
       type,
       event_format,
       event_date,
       event_time,
       cast(null as numeric)     as budget,
       cast(null as numeric)     as cost,
       cast(null as numeric)     as expected_revenue
from {{ ref('stg_eventbrite_events') }}
