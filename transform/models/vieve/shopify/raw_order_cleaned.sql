/*
CONTEXT:
- Taking the raw order table from shopify and joining it to google sheet that contains App ID
- We remove orders where they are test orders or exchanged ordered
RESULT EXPECTATION:
- Unqiue by id aka order_id
ASSUMPTION:
-
*/

{{ config(materialized='table') }}

SELECT
CAST(DATETIME(orders.created_at, "Europe/London") AS DATE) as order_date
, orders.*
, SAFE_CAST(JSON_EXTRACT_SCALAR(original_total_duties_set, '$.shop_money.amount') AS FLOAT64) AS duties
FROM {{source('shopify', 'orders')}} orders
WHERE 
    test is false 
        -- and app_id is not in (129785)