select distinct generate_uuid() as event_sk,
       event_id,
       event_name,
       description,
       type,
       event_format,
       event_date,
       event_time,
       budget,
       cost,
       expected_revenue
from {{ref('event_detail')}}
