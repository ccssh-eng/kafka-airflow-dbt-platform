{{ config(materialized='table') }}

SELECT
    customer_id,
    MAX(customer_name) AS customer_name,
    MAX(region) AS region,
    COUNT(order_id) AS nb_commandes,
    SUM(amount) AS montant_total,
    ROUND(AVG(amount), 2) AS montant_moyen,
    MIN(order_date) AS premiere_commande,
    MAX(order_date) AS derniere_commande
FROM {{ ref('orders_clean') }}
GROUP BY customer_id
ORDER BY montant_total DESC

