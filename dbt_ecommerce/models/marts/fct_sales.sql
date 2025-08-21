WITH orders AS (
    SELECT
        *
    FROM {{ref('stg_orders')}}
    WHERE status = 'completo'
),

items AS (
    SELECT 
        *
    FROM {{ref('stg_order_items')}}
),

products AS (
    SELECT
        *
    FROM {{ref('stg_products')}}
),

joined AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        i.product_id,
        i.quantity,
        p.price,
        p.cost,
        p.margin,
        (i.quantity * p.price) AS revenue,
        (i.quantity * p.margin) AS profit
    FROM orders o
    INNER JOIN items i ON o.order_id = i.order_id
    INNER JOIN products p ON i.product_id = p.product_id
)

SELECT
    *
FROM joined