/*
CONTEXT:
-- Customer table, contains customer metrics at the email address level
-- Takes into account data across both shopify stores
RESULT EXPECTATION:
- Unique by Email 
ASSUMPTION:
-
*/

{{ config(materialized='table') }}

WITH email_table AS (
    
    SELECT
        id AS order_id
        , customer__id as customer_id
        , user_id
        , lower(email) AS email
        , order_date
        FROM {{ref('raw_order_cleaned')}} orders 
) ,

last_address AS (

    SELECT
        customer_id
        , email
        , billing_address_country
    FROM
        (SELECT
            lower(email) AS email
            , customer__id as customer_id
            , billing_address__country as billing_address_country
            , order_date
            FROM {{ref('raw_order_cleaned')}} orders           
        )
        WHERE billing_address_country IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY email ORDER BY order_date DESC) = 1

)
, email_sales_table AS (
    SELECT
        email_table.email
        , billing_address_country
        -- , sum(orders_table.items) AS items
        , COUNT(DISTINCT orders_table.orders_order_id) AS orders
        -- , sum(orders_table.returned_orders) AS returned_orders
        -- , sum(orders_table.net_orders) AS net_orders
        -- , sum(orders_table.returned_items) AS returned_items
        -- , sum(orders_table.gift_card_redeemed_value) AS gift_card_redeemed_value
        -- , sum(orders_table.total_sales) AS total_sales
        -- , sum(orders_table.sales) AS sales 
        -- , sum(orders_table.total_sales_plus_returns) AS total_sales_plus_returns
        -- , sum(orders_table.gross_sales) AS gross_sales
        -- , sum(orders_table.net_sales) AS net_sales
        -- , sum(orders_table.net_sales_plus_returns) AS net_sales_plus_returns
    FROM {{ref('shopify_key_metrics')}} orders_table
    LEFT JOIN email_table 
        ON orders_table.order_id = email_table.order_id
    LEFT JOIN last_address 
        ON email_table.customer_id = last_address.customer_id
    GROUP BY ALL
)
SELECT
    email
    , billing_address_country AS billing_order_country 
    , CASE WHEN orders > 1 THEN 'Returning customer' ELSE 'New customer' END AS customer_type
    -- , CASE WHEN IFNULL(items,0) + IFNULL(returned_items,0) = 0 THEN 0 ELSE 1 END AS has_kept_order_flag
    -- , CASE WHEN gift_card_order > 1 AND gift_card_order = orders THEN 1 ELSE 0 END AS gift_card_only_customer_flag
    -- , items
    -- , returned_items
    , orders
    -- , returned_orders
    -- , net_orders
    -- , gift_card_order
    -- , gift_card_redeemed_value
    -- , total_sales
    -- , sales 
    -- , total_sales_plus_returns
    -- , gross_sales
    -- , net_sales
    -- , net_sales_plus_returns
FROM email_sales_table