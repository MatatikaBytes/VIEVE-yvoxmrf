/*
CONTEXT:
- Getting the first order and date of customer using email
RESULT EXPECTATION:
- Unique by Email and order_id
ASSUMPTION:
-
*/

{{ 
    config(
        materialized='table',
        schema = 'matatika_shopify_marts'  
    ) 
}}

SELECT
    id as order_id
    , customer__id as customer_id
    , user_id
    , lower(email) as email
    , order_date
FROM {{ref('raw_order_cleaned')}}  orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at ASC) = 1 