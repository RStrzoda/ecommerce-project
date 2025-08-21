SELECT
    product_id,
    product_name,
    category,
    price,
    cost,
    margin
FROM {{ref('stg_products')}}