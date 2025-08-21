{{ config (materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM {{ref("cancelamentos")}}
),

renamed AS (
    SELECT
        order_id,
        cancel_reason,
        CAST(cancel_date AS DATE) AS cancel_date
    FROM source
)

SELECT
    *
FROM renamed