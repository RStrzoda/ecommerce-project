WITH base AS (
    SELECT
        *
    FROM {{ref('stg_customers')}}
),

churn_date AS (
    SELECT
        *
    FROM {{ref('int_churn_prediction_data')}}
)

SELECT
    b.customer_id,
    b.customer_name,
    b.email,
    b.signup_date,
    c.total_orders,
    c.total_cancelled,
    c.days_between,
    c.is_churn
FROM base b
LEFT JOIN churn_date c ON b.customer_id = c.customer_id