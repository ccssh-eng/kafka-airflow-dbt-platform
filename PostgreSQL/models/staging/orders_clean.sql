{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    amount,
    status,
    order_date,
    customer_name,
    region,
    email,
    created_at
FROM {{ source('raw', 'orders') }}

