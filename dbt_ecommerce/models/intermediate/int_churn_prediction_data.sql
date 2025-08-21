WITH orders AS (
    SELECT
        *
    FROM {{ref('stg_orders')}}
),

aggregated_orders AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        MIN(order_date) AS first_order,
        MAX(order_date) AS last_order,
        (MAX(order_date) - MIN(order_date)) AS days_between,
        COUNT(CASE WHEN status = 'cancelado' THEN 1 ELSE NULL END) AS total_cancelled
    FROM orders
    GROUP BY customer_id
),

churned_flag AS (
    SELECT
        *,
        CASE WHEN (CURRENT_DATE - last_order) > 90 THEN 1 ELSE 0 END AS is_churn
    FROM aggregated_orders
)

SELECT
    *
FROM churned_flag