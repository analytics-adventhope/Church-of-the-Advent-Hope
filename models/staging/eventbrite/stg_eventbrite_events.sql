-- stg_eventbrite_events.sql
-- Cleans and standardizes raw Eventbrite event data
-- Source: Fivetran sync → eventbrite_raw.event
-- Grain: one row per event

WITH source AS (
    -- filter out soft-deleted records from Fivetran
    SELECT *
    FROM `analytics-473100.eventbrite_raw.event`
    WHERE _fivetran_deleted = FALSE
)

SELECT
    CAST(id AS STRING) AS eventbrite_event_id, -- cast to STRING for compatibility with Salesforce IDs
    name_text AS event_name,
    description_text AS description,
    'Eventbrite' AS event_source,
    CASE -- classify event type based on name to align with Salesforce Campaign type values
        WHEN LOWER(name_text) LIKE '%worship%' THEN 'Worship Service' 
        WHEN LOWER(name_text) LIKE '%prayer%' THEN 'Prayer'
        WHEN LOWER(name_text) LIKE '%bible%' OR LOWER(name_text) LIKE '%study%' THEN 'Bible Study'
        ELSE 'Event'
    END AS type,
    CASE -- flag for online vs in-person, future-proofed for virtual events
        WHEN online_event = TRUE THEN 'Online'
        ELSE 'In-Person'
    END AS event_format,
    DATE(start_local) AS event_date,
    FORMAT_DATETIME('%I:%M %p', start_local) AS event_time, -- e.g. '07:00 PM'
    start_local AS event_start_at,
    end_local AS event_end_at,
    venue_id,
    capacity,
    status,
    is_free,
    _fivetran_synced AS synced_at
FROM source
WHERE status != 'draft' -- exclude unpublished events
ORDER BY event_date DESC