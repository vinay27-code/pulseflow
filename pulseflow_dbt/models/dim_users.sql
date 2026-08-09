-- User-level aggregates

{{ config(materialized='table') }}

SELECT
    user_id,
    COUNT(DISTINCT session_id)                                          AS total_sessions,
    COUNT(*)                                                            AS total_events,
    COUNT(*) FILTER (WHERE event_type = 'product_view')                AS product_views,
    COUNT(*) FILTER (WHERE event_type = 'add_to_cart')                 AS add_to_carts,
    COUNT(*) FILTER (WHERE event_type = 'payment_success')             AS successful_payments,
    COALESCE(SUM(amount) FILTER (WHERE event_type = 'payment_success'), 0) AS total_revenue,
    MIN(event_timestamp)                                                AS first_seen_at,
    MAX(event_timestamp)                                                AS last_seen_at
FROM {{ ref('stg_events') }}
WHERE user_id IS NOT NULL
GROUP BY user_id
