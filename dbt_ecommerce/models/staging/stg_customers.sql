{{ config (materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM {{ref("customers")}}
),

renamed AS (
    SELECT
        customer_id,
        LOWER(TRIM(name)) AS customer_name,
        LOWER(TRIM(email)) AS email,
        CAST(signup_date AS DATE) AS signup_date
    FROM source
)

SELECT
    *
FROM renamed