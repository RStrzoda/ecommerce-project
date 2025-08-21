{{ config (materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM {{ref("products")}}
),

renamed AS (
    SELECT
        product_id,
        INITCAP(LOWER(name)) AS product_name,
        category,
        CAST(price AS NUMERIC) AS price,
        CAST(custo AS NUMERIC) AS cost,
        ROUND(CAST(price - custo AS NUMERIC), 2) AS margin
    FROM source
)

SELECT
    *
FROM renamed