-- Order-level facts with revenue metrics

{{ config(materialized='table') }}

SELECT
    order_id,
    user_id,
    currency,
    MAX(amount) FILTER (WHERE event_type = 'payment_success') AS revenue,
    MIN(event_timestamp) FILTER (WHERE event_type = 'checkout') AS checkout_at,
    MIN(event_timestamp) FILTER (WHERE event_type = 'payment_success') AS paid_at,
    MIN(event_timestamp) FILTER (WHERE event_type = 'order_created') AS order_created_at,
    BOOL_OR(event_type = 'payment_failure') AS had_payment_failure,
    BOOL_OR(event_type = 'order_cancelled') AS was_cancelled,
    COUNT(*) AS event_count
FROM {{ ref('stg_events') }}
WHERE order_id IS NOT NULL
GROUP BY order_id, user_id, currency
