-- doit retourner 0 ligne pour passer
SELECT *
FROM {{ ref('orders_clean') }}
WHERE amount <= 0

