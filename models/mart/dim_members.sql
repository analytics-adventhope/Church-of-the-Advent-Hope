with worship_events as (
  select count(distinct case when (date_diff(current_date('EST'), event_date, day) / 30.0 <= 3)
                             then event_date end) as recent_worship,
         count(distinct case when (date_diff(current_date('EST'), event_date, day) / 30.0 <= 6) and
                                  (date_diff(current_date('EST'), event_date, day) / 30.0 > 3)
                             then event_date
                         end)
               as baseline_worship,
    from {{ref('event_detail')}}
    where type = 'Worship Service'
),

member_attendance as (
  select distinct
         contact_id,
         count(distinct case when (date_diff(current_date('EST'), event_date, day) / 30.0 <= 3)
                             then event_date end) as recent_attendance,
         count(distinct case when (date_diff(current_date('EST'), event_date, day) / 30.0 <= 6) and
                                  (date_diff(current_date('EST'), event_date, day) / 30.0 > 3)
                             then event_date
                         end)
               as baseline_attendance,
    from {{ref('event_detail')}}
    where type = 'Worship Service'
    group by 1
),

risk_profiling as (
  select contact_id,
         safe_divide(recent_attendance, recent_worship) as recent_rate,
         safe_divide(baseline_attendance, baseline_worship) as baseline_rate
  from member_attendance
  cross join worship_events
),

stats_summary as (
  select avg(recent_rate) as mean_recent,
         stddev(recent_rate) as sd_recent
  from risk_profiling
),

risk as (
  select r.contact_id,
         case when baseline_rate >= 0.7 and recent_rate <= 0.3 then 'High'
              when baseline_rate >= 0.7 and recent_rate > 0.3 and recent_rate <= 0.6 then 'Medium'
              when baseline_rate >= 0.7 and recent_rate > 0.6 then 'Low'
              ELSE 'Not Applicable'
         end as rule_based_flag,
         case when (r.recent_rate - s.mean_recent) / nullif(s.sd_recent, 0) <= -1.5 then 'High'
              when (r.recent_rate - s.mean_recent) / nullif(s.sd_recent, 0) > -1.5 
                   and (r.recent_rate - s.mean_recent) / nullif(s.sd_recent, 0) <= -0.5 then 'Medium'
              when (r.recent_rate - s.mean_recent) / nullif(s.sd_recent, 0) > -0.5 then 'Low'
              else 'Not Applicable'
         end as statistical_flag
  from risk_profiling r
  cross join stats_summary s
),

hybrid_risk as (
  select rb.contact_id,
       --   rb.baseline_rate,
       --   rb.recent_rate,
       --   rb.drop_ratio,
         rb.rule_based_flag,
       --   sr.z_score,
         rb.statistical_flag,
         case when rb.rule_based_flag = 'High'   or rb.statistical_flag = 'High'   then 'High'
              when rb.rule_based_flag = 'Medium' or rb.statistical_flag = 'Medium' then 'Medium'
              when rb.rule_based_flag = 'Low'    and rb.statistical_flag = 'Low'   then 'Low'
              else 'Not Applicable'
         end as hybrid_flag
  from risk rb
),

latest_worship as (
  select contact_id,
         max(event_date) as latest_worship_date
  from {{ref('event_detail')}}
  where type = 'Worship Service'
  group by 1
)

select distinct ed.contact_id,
       first_name,
       last_name,
       salutation,
       phone,
       mobile_phone,
       email,
       fax,
       country,
       state_,
       city,
       street,
       postal_code,
       company,
       participant_type,
       opted_out_of_email,
       lw.latest_worship_date,
       hr.rule_based_flag,
       hr.statistical_flag,
       hr.hybrid_flag
from {{ref('event_detail')}} ed
left join latest_worship lw
       on ed.contact_id = lw.contact_id
left join hybrid_risk hr
       on ed.contact_id = hr.contact_id
