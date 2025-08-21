{{ config (materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM {{ref("orders")}}
),

renamed AS (
    SELECT
        order_id,
        customer_id,
        CAST(order_date AS DATE) AS order_date,
        LOWER(TRIM(status)) AS status
    FROM source
)

SELECT
    *
FROM renamed