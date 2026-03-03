-- stg_eventbrite_orders.sql
-- Cleans and standardizes raw Eventbrite order (ticket purchase) data
-- Source: Fivetran sync -> eventbrite_raw.orders
-- Grain: one row per order (one order can contain multiple attendees)

WITH source AS (
    -- filter out soft-deleted records from Fivetran
    SELECT *
    FROM `analytics-473100.eventbrite_raw.orders`
    WHERE _fivetran_deleted = FALSE
)

SELECT
    CAST(id AS STRING) AS eventbrite_order_id, -- cast to STRING for consistency across staging models
    CAST(event_id AS STRING) AS eventbrite_event_id,
    owner_first_name AS first_name, -- order owner, may differ from attendee if someone buys tickets for others
    owner_last_name AS last_name,
    LOWER(TRIM(owner_email)) AS email, -- normalized for potential identity resolution
    status,
    gross_major_value AS order_total, -- total amount paid including fees, in major currency units (e.g. dollars not cents)
    gross_currency AS currency,
    created AS ordered_at,
    _fivetran_synced AS synced_at
FROM source
ORDER BY ordered_at DESC