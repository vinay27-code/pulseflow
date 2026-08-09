-- Hourly conversion funnel metrics

{{ config(materialized='table') }}

WITH hourly AS (
    SELECT
        DATE_TRUNC('hour', event_timestamp) AS hour_bucket,
        COUNT(*) FILTER (WHERE event_type = 'product_view')    AS product_views,
        COUNT(*) FILTER (WHERE event_type = 'add_to_cart')     AS add_to_carts,
        COUNT(*) FILTER (WHERE event_type = 'checkout')        AS checkouts,
        COUNT(*) FILTER (WHERE event_type = 'payment_success') AS payments_success,
        COUNT(*) FILTER (WHERE event_type = 'order_created')   AS orders_created,
        COUNT(DISTINCT user_id)                                AS unique_users,
        COALESCE(SUM(amount) FILTER (WHERE event_type = 'payment_success'), 0) AS revenue
    FROM {{ ref('stg_events') }}
    GROUP BY 1
)
SELECT
    hour_bucket,
    product_views,
    add_to_carts,
    checkouts,
    payments_success,
    orders_created,
    unique_users,
    revenue,
    ROUND(
        CASE WHEN product_views > 0
        THEN add_to_carts::decimal / product_views * 100
        ELSE 0 END, 2
    ) AS view_to_cart_pct,
    ROUND(
        CASE WHEN checkouts > 0
        THEN payments_success::decimal / checkouts * 100
        ELSE 0 END, 2
    ) AS checkout_conversion_pct
FROM hourly
ORDER BY hour_bucket DESC
