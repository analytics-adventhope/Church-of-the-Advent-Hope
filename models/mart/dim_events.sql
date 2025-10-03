select distinct generate_uuid() as event_sk,
       id,
       name,
       description,
       type,
       event_format,
       event_date,
       event_time,
       budget,
       cost,
       expected_revenue
from {{ref('event_detail')}}
