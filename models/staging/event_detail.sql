select distinct c.Id as event_id,
       c.Name as event_name,
       c.Description as description,
       c.Type as type,
       c.Format__c as event_format,
       c.EndDate as event_date,
       c.Service_Time__c as event_time,
       c.BudgetedCost as budget,
       c.ActualCost as cost,
       c.ExpectedRevenue as expected_revenue,
       cm.ContactId as contact_id,
       cm.FirstName as first_name,
       cm.LastName as last_name,
       cm.Salutation as salutation,
       cm.Phone as phone,
       cm.MobilePhone as mobile_phone,
       cm.Email as email,
       cm.Fax as fax,
       cm.Country as country,
       cm.State as state_,
       cm.City as city,
       cm.Street as street,
       cm.PostalCode as postal_code,
       cm.CompanyOrAccount as company,
       cm.Participant_Type__c as participant_type,
       cm.HasOptedOutOfEmail as opted_out_of_email,
       cm.Status as status,
       cm.Attendance_Indicator__c as attendance_indicator,
       case when Status in ('Attended', 'Attendeed') then 'In-Person'
            when Status = 'Attended Online' then 'Online'
            else Status
       end as attendance_type
       -- case when cm.Attendee_Type__c is null then 'Online' else cm.Attendee_Type__c end as attendance_type,
      --  cm.Service__c, --> has a null
      -- wishlist: engagement score
from {{ source('raw_attendance', 'Campaign') }} c --"analytics-473100.raw_attendance.Campaign" c
inner join {{ source('raw_attendance', 'CampaignMember') }} cm --"analytics-473100.raw_attendance.CampaignMember" cm
        on c.Id = cm.CampaignId
