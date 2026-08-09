-- Staging layer: clean and deduplicated events
-- This is the foundation all other models build on

{{ config(
    materialized='incremental',
    unique_key='event_id',
    indexes=[
        {'columns': ['event_timestamp'], 'type': 'btree'},
        {'columns': ['event_type'], 'type': 'btree'},
        {'columns': ['user_id'], 'type': 'btree'}
    ]
) }}

SELECT
    event_id,
    event_type,
    user_id,
    session_id,
    product_id,
    order_id,
    amount,
    currency,
    properties,
    event_timestamp,
    received_at,
    (properties->>'_late_arriving')::boolean AS is_late_arriving,
    (properties->>'_age_hours')::float AS late_arriving_hours
FROM {{ source('pulseflow', 'raw_events') }}
WHERE is_valid = TRUE
  AND is_duplicate = FALSE

{% if is_incremental() %}
  AND received_at > (SELECT MAX(received_at) FROM {{ this }})
{% endif %}
