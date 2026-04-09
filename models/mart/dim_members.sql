-- ============================================================
-- WORSHIP ATTENDANCE RISK PROFILING
-- ============================================================

with worship_events as (
  select count(distinct case when (date_diff(current_date('EST'), event_date, day) / 30.0 <= 3)
                             then event_date end) as recent_worship,
         count(distinct case when (date_diff(current_date('EST'), event_date, day) / 30.0 <= 6) and
                                  (date_diff(current_date('EST'), event_date, day) / 30.0 > 3)
                             then event_date end) as baseline_worship
    from {{ ref('event_detail') }}
    where type = 'Worship Service'
),

member_attendance as (
  select distinct
         contact_id,
         count(distinct case when (date_diff(current_date('EST'), event_date, day) / 30.0 <= 3)
                             then event_date end) as recent_attendance,
         count(distinct case when (date_diff(current_date('EST'), event_date, day) / 30.0 <= 6) and
                                  (date_diff(current_date('EST'), event_date, day) / 30.0 > 3)
                             then event_date end) as baseline_attendance
    from {{ ref('event_detail') }}
    where type = 'Worship Service'
      and participant_type in ('Homer', 'Elder', 'Ecclesia', 'Deacon')
    group by 1
),

risk_profiling as (
  select contact_id,
         safe_divide(recent_attendance, recent_worship)                          as recent_rate,
         safe_divide(baseline_attendance, baseline_worship)                      as baseline_rate,
         safe_divide(baseline_attendance - recent_attendance, baseline_attendance) as drop_ratio
  from member_attendance
  cross join worship_events
),

stats_summary as (
  select avg(recent_rate)    as mean_recent,
         stddev(recent_rate) as sd_recent
  from risk_profiling
),

risk as (
  select r.contact_id,
         baseline_rate,
         recent_rate,
         case
              when baseline_rate >= 0.7 and drop_ratio >= 0.5                     then 'High'
              when baseline_rate between 0.4 and 0.7 and drop_ratio >= 0.6        then 'Medium'
              when baseline_rate < 0.4 and drop_ratio < 0.2                       then 'Low'
              else 'Not Applicable'
         end as rule_based_flag,
         case when (r.recent_rate - s.mean_recent) / nullif(s.sd_recent, 0) <= -1.0   then 'High'
              when (r.recent_rate - s.mean_recent) / nullif(s.sd_recent, 0) > -1.0
                   and (r.recent_rate - s.mean_recent) / nullif(s.sd_recent, 0) <= -0.5 then 'Medium'
              when (r.recent_rate - s.mean_recent) / nullif(s.sd_recent, 0) > -0.5   then 'Low'
              else 'Not Applicable'
         end as statistical_flag
  from risk_profiling r
  cross join stats_summary s
  where baseline_rate > 0
    and not (baseline_rate < 0.20 and recent_rate < 0.20)
),

hybrid_risk as (
  select rb.contact_id,
         rb.baseline_rate,
         rb.recent_rate,
         rb.rule_based_flag,
         rb.statistical_flag,
         case when rb.rule_based_flag = 'High'   and rb.statistical_flag = 'High'   then 'High'
              when rb.rule_based_flag = 'High'   and rb.statistical_flag = 'Medium' then 'Medium'
              when rb.rule_based_flag = 'Medium' and rb.statistical_flag = 'High'   then 'Medium'
              when rb.rule_based_flag = 'High'   and rb.statistical_flag = 'Low'    then 'Medium'
              when rb.rule_based_flag = 'Low'    and rb.statistical_flag = 'High'   then 'Medium'
              when rb.rule_based_flag = 'Medium' and rb.statistical_flag = 'Medium' then 'Medium'
              when rb.rule_based_flag = 'Low'    and rb.statistical_flag = 'Medium' then 'Medium'
              when rb.rule_based_flag = 'Medium' and rb.statistical_flag = 'Low'    then 'Medium'
              when rb.rule_based_flag = 'Low'    and rb.statistical_flag = 'Low'    then 'Low'
              else 'Not Applicable'
         end as hybrid_flag
  from risk rb
),

latest_worship as (
  select contact_id,
         max(event_date) as latest_worship_date,
         count(distinct case when type = 'Worship Service' and attendance_type = 'In-Person' then event_date end)
               / count(distinct case when type = 'Worship Service' then event_date end) as in_person_prop,
         count(distinct case when type = 'Worship Service' and event_time = '11:30 AM' then event_date end)
               / count(distinct case when type = 'Worship Service' then event_date end) as second_service_prop
  from {{ ref('event_detail') }}
  where type = 'Worship Service'
  group by 1
),

-- ============================================================
-- FIRST EVENT TRACKING
-- ============================================================

first_event_sf as (
    select
        contact_id,
        min(event_date)  as first_event_date,
        'Worship'        as first_event_name
    from {{ ref('event_detail') }}
    group by 1
),

first_event_eb as (
    select
        lower(trim(a.email))                                                      as email_key,
        min(e.event_date)                                                         as first_event_date,
        array_agg(e.event_name order by e.event_date asc limit 1)[offset(0)]     as first_event_name
    from {{ ref('stg_eventbrite_attendees') }} a
    inner join {{ ref('stg_eventbrite_events') }} e
            on a.eventbrite_event_id = e.eventbrite_event_id
    group by 1
),

-- ============================================================
-- IDENTITY RESOLUTION
-- Tier 1 — email match: handled implicitly (EB contacts with same email as SF never reach eb_only_contacts)
-- Tier 2 — exact name match (lowered + trimmed): 1-to-1 → merge, many-to-1 → ambiguous
-- Tier 3 — name bigram similarity for contacts not resolved by Tier 2: medium/low flag only
-- ============================================================

sf_contacts as (
    select
        contact_id,
        any_value(first_name)           as first_name,
        any_value(last_name)            as last_name,
        any_value(lower(trim(email)))   as email
    from {{ ref('event_detail') }}
    where first_name is not null
      and last_name  is not null
    group by contact_id
),

eb_candidates as (
    select
        lower(trim(a.email))        as email_key,
        any_value(a.first_name)     as first_name,
        any_value(a.last_name)      as last_name
    from {{ ref('stg_eventbrite_attendees') }} a
    left join (
        select distinct lower(trim(email)) as email
        from {{ ref('event_detail') }}
        where email is not null
    ) sf_e on lower(trim(a.email)) = sf_e.email
    where sf_e.email is null
      and a.email is not null
      and trim(a.email) not in ('info requested', '', 'n/a', 'none')
      and a.email like '%@%'
      and a.first_name is not null and trim(a.first_name) != ''
      and a.last_name  is not null and trim(a.last_name)  != ''
    group by lower(trim(a.email))
),

name_matched as (
    select
        eb.email_key                                                              as eb_email,
        sf.contact_id                                                             as sf_contact_id,
        count(distinct sf.contact_id) over (
            partition by lower(trim(eb.first_name)), lower(trim(eb.last_name))
        )                                                                         as n_sf_same_name
    from eb_candidates eb
    inner join sf_contacts sf
        on  lower(trim(eb.first_name)) = lower(trim(sf.first_name))
        and lower(trim(eb.last_name))  = lower(trim(sf.last_name))
),

tier2 as (
    select
        eb_email,
        sf_contact_id,
        n_sf_same_name,
        cast(null as float64)                                                     as name_similarity,
        case when n_sf_same_name > 1 then 'ambiguous' else 'high' end            as match_confidence
    from name_matched
),

eb_unresolved as (
    select * from eb_candidates
    where email_key not in (select eb_email from name_matched)
),

name_similarity_raw as (
    select
        eb.email_key                                                              as eb_email,
        sf.contact_id                                                             as sf_contact_id,
        lower(trim(eb.first_name)) || ' ' || lower(trim(eb.last_name))           as eb_full_name,
        lower(trim(sf.first_name)) || ' ' || lower(trim(sf.last_name))           as sf_full_name
    from eb_unresolved eb
    cross join sf_contacts sf
),

name_bigrams as (
    select
        eb_email,
        sf_contact_id,
        (select count(distinct bg)
         from unnest(array(
             select substr(eb_full_name, i, 2)
             from unnest(generate_array(1, greatest(length(eb_full_name) - 1, 0))) as i
         )) as bg
         where length(bg) = 2
           and bg in unnest(array(
             select substr(sf_full_name, i, 2)
             from unnest(generate_array(1, greatest(length(sf_full_name) - 1, 0))) as i
           ))
        ) as shared_bigrams,
        (select count(distinct bg)
         from unnest(array(
             select substr(eb_full_name, i, 2)
             from unnest(generate_array(1, greatest(length(eb_full_name) - 1, 0))) as i
         )) as bg
         where length(bg) = 2
        ) as eb_bigram_count,
        (select count(distinct bg)
         from unnest(array(
             select substr(sf_full_name, i, 2)
             from unnest(generate_array(1, greatest(length(sf_full_name) - 1, 0))) as i
         )) as bg
         where length(bg) = 2
        ) as sf_bigram_count
    from name_similarity_raw
),

tier3 as (
    select
        eb_email,
        sf_contact_id,
        1                                                                         as n_sf_same_name,
        round(safe_divide(shared_bigrams, greatest(eb_bigram_count, sf_bigram_count, 1)), 2)
                                                                                  as name_similarity,
        case
            when safe_divide(shared_bigrams, greatest(eb_bigram_count, sf_bigram_count, 1)) >= 0.6 then 'medium'
            when safe_divide(shared_bigrams, greatest(eb_bigram_count, sf_bigram_count, 1)) >= 0.4 then 'low'
            else null
        end                                                                       as match_confidence
    from name_bigrams
    qualify row_number() over (
        partition by eb_email
        order by safe_divide(shared_bigrams, greatest(eb_bigram_count, sf_bigram_count, 1)) desc,
                 sf_contact_id asc
    ) = 1
),

name_resolution as (
    select * from tier2
    union all
    select * from tier3 where match_confidence is not null
),

high_conf as (
    select sf_contact_id, eb_email
    from name_resolution
    where match_confidence = 'high'
),

flagged_matches as (
    select
        sf_contact_id,
        concat('eb_', to_hex(md5(eb_email)))                                      as eb_contact_id,
        any_value(name_similarity)                                                 as name_similarity
    from name_resolution
    where match_confidence in ('medium', 'low')
      and n_sf_same_name = 1
    group by 1, 2
),

-- ============================================================
-- SALESFORCE MEMBERS
-- ============================================================

sf_members as (
  select
      ed.contact_id,
      initcap(lower(trim(any_value(ed.first_name))))                              as first_name,
      initcap(lower(trim(any_value(ed.last_name))))                               as last_name,
      any_value(ed.salutation)                                                    as salutation,
      any_value(ed.phone)                                                         as phone,
      any_value(ed.mobile_phone)                                                  as mobile_phone,
      any_value(lower(trim(ed.email)))                                            as email_1,
      any_value(hc.eb_email)                                                      as email_2,
      any_value(ed.fax)                                                           as fax,
      any_value(ed.country)                                                       as country,
      any_value(ed.state_)                                                        as state_,
      any_value(ed.city)                                                          as city,
      any_value(ed.street)                                                        as street,
      any_value(ed.postal_code)                                                   as postal_code,
      any_value(ed.company)                                                       as company,
      array_agg(ed.participant_type order by ed.event_date desc limit 1)[offset(0)] as participant_type,
      any_value(ed.opted_out_of_email)                                            as opted_out_of_email,
      any_value(lw.latest_worship_date)                                           as latest_worship_date,
      any_value(hr.baseline_rate)                                                 as baseline_rate,
      any_value(hr.recent_rate)                                                   as recent_rate,
      any_value(case when hr.contact_id is not null then hr.rule_based_flag  else 'Not Applicable' end) as rule_based_flag,
      any_value(case when hr.contact_id is not null then hr.statistical_flag else 'Not Applicable' end) as statistical_flag,
      any_value(case when hr.contact_id is not null then hr.hybrid_flag      else 'Not Applicable' end) as hybrid_flag,
      any_value(lw.in_person_prop)                                               as in_person_prop,
      any_value(lw.second_service_prop)                                          as second_service_prop,
      case
          when any_value(fe_eb.first_event_date) is not null
               and any_value(fe_eb.first_event_date) < any_value(fe_sf.first_event_date)
          then any_value(fe_eb.first_event_date)
          else any_value(fe_sf.first_event_date)
      end                                                                         as first_event_date,
      case
          when any_value(fe_eb.first_event_date) is not null
               and any_value(fe_eb.first_event_date) < any_value(fe_sf.first_event_date)
          then any_value(fe_eb.first_event_name)
          when any_value(fe_sf.first_event_date) is not null
          then 'Worship'
          else null
      end                                                                         as first_event_name,
      any_value(fm.eb_contact_id)                                                 as possible_duplicate_contact_id,
      any_value(fm.name_similarity)                                               as name_similarity
  from {{ ref('event_detail') }} ed
  left join latest_worship lw    on ed.contact_id  = lw.contact_id
  left join hybrid_risk hr       on ed.contact_id  = hr.contact_id
  left join first_event_sf fe_sf on ed.contact_id  = fe_sf.contact_id
  left join high_conf hc         on ed.contact_id  = hc.sf_contact_id
  left join first_event_eb fe_eb on hc.eb_email    = fe_eb.email_key
  left join flagged_matches fm   on ed.contact_id  = fm.sf_contact_id
  group by all
  having any_value(ed.first_name) is not null and trim(any_value(ed.first_name)) != ''
     and any_value(ed.last_name)  is not null and trim(any_value(ed.last_name))  != ''
),

-- ============================================================
-- EVENTBRITE-ONLY CONTACTS
-- ============================================================

eb_normalized as (
  select
      lower(trim(email)) as email_key,
      first_name,
      last_name,
      phone_raw
  from {{ ref('stg_eventbrite_attendees') }}
  where email is not null
    and trim(email) not in ('info requested', '', 'n/a', 'none')
    and email like '%@%'
    and first_name is not null and trim(first_name) != ''
    and last_name  is not null and trim(last_name)  != ''
),

eb_only_contacts as (
  select
      concat('eb_', to_hex(md5(n.email_key)))                                    as contact_id,
      initcap(lower(trim(any_value(n.first_name))))                              as first_name,
      initcap(lower(trim(any_value(n.last_name))))                               as last_name,
      cast(null as string)                                                        as salutation,
      any_value(n.phone_raw)                                                      as phone,
      cast(null as string)                                                        as mobile_phone,
      n.email_key                                                                 as email_1,
      cast(null as string)                                                        as email_2,
      cast(null as string)                                                        as fax,
      cast(null as string)                                                        as country,
      cast(null as string)                                                        as state_,
      cast(null as string)                                                        as city,
      cast(null as string)                                                        as street,
      cast(null as string)                                                        as postal_code,
      cast(null as string)                                                        as company,
      cast(null as string)                                                        as participant_type,
      cast(null as bool)                                                          as opted_out_of_email,
      cast(null as date)                                                          as latest_worship_date,
      cast(null as float64)                                                       as baseline_rate,
      cast(null as float64)                                                       as recent_rate,
      'Not Applicable'                                                            as rule_based_flag,
      'Not Applicable'                                                            as statistical_flag,
      'Not Applicable'                                                            as hybrid_flag,
      cast(null as float64)                                                       as in_person_prop,
      cast(null as float64)                                                       as second_service_prop,
      any_value(fe_eb.first_event_date)                                           as first_event_date,
      any_value(fe_eb.first_event_name)                                           as first_event_name,
      any_value(case
          when nr.match_confidence in ('medium', 'low') and nr.n_sf_same_name = 1
          then nr.sf_contact_id
          else null
      end)                                                                        as possible_duplicate_contact_id,
      any_value(nr.name_similarity)                                               as name_similarity
  from eb_normalized n
  left join (
      select distinct lower(trim(email)) as email
      from {{ ref('event_detail') }}
      where email is not null
  ) sf_e on n.email_key = sf_e.email
  left join high_conf hc          on n.email_key = hc.eb_email
  left join name_resolution nr    on n.email_key = nr.eb_email
  left join first_event_eb fe_eb  on n.email_key = fe_eb.email_key
  where sf_e.email is null
    and hc.eb_email is null
  group by n.email_key
)

select * from sf_members
union all
select * from eb_only_contacts
