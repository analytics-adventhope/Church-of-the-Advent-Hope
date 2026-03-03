-- stg_eventbrite_attendees.sql
-- Cleans and standardizes raw Eventbrite attendee (registration) data
-- Source: Fivetran sync -> eventbrite_raw.attendee
-- Grain: one row per attendee per event

WITH source AS (
    -- filter out soft-deleted, cancelled, and refunded registrations
    -- only keep attendees with valid, active registrations
    SELECT *
    FROM `analytics-473100.eventbrite_raw.attendee`
    WHERE _fivetran_deleted = FALSE
        AND cancelled = FALSE
        AND refunded = FALSE
)

SELECT
    CAST(id AS STRING) AS eventbrite_attendee_id, -- cast to STRING for compatibility with Salesforce IDs
    CAST(event_id AS STRING) AS eventbrite_event_id,
    CAST(order_id AS STRING) AS eventbrite_order_id,
    profile_first_name AS first_name,
    profile_last_name AS last_name,
    LOWER(TRIM(profile_email)) AS email, -- normalize for identity resolution against Salesforce contacts
    REGEXP_REPLACE(profile_cell_phone, r'[^0-9]', '') AS phone_normalized, -- strip non-numeric chars (parens, dashes, spaces) for matching
    profile_cell_phone AS phone_raw, -- preserve original format for display/contact purposes
    checked_in, -- TRUE = physically attended, critical for attendance metrics
    status,
    created AS registered_at,
    _fivetran_synced AS synced_at
FROM source