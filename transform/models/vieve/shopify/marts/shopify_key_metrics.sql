/*
CONTEXT:
- Creating a table that matches the Shopify analytics output
  - Link https://admin.shopify.com/store/hylo-shoes/reports/2582642788?since=2023-01-01&until=2023-01-31&over=day
- The returns logic is based on the day the return was made
  - e.g. I could order on 01/01/2023 the will an order id if I return on the 03/01/2023 there will be another order_id line with the return information
- It shows actual movements like a log, so the date when order was placed and actual date the refund was made
RESULT EXPECTATION:
- There is a line by day and order id / return order id
ASSUMPTION:
-
*/

{{ 
    config(
        materialized='table',
        schema = 'marts'  
    ) 
}}
-- using cleaned order table that handles exchanged orders

WITH order_line_agg AS (
  SELECT
    order_line.order_id
    , max(rate) AS tax_rate
    , SUM(gross_sales) AS gross_sales
    , SUM(quantity) AS quantity
  FROM {{ref('order_line_product')}}  order_line --`hylo-data.hylo_fivetran_shopify.order_line`
  GROUP BY ALL
) ,

orders_table AS (

SELECT
orders.order_date AS order_date
, orders.id AS order_id
, orders.name AS order_name
, orders.app_id
, orders.user_id AS user_id
, orders.customer__id AS customer_id
, orders.email AS email
-- , discount_code.code
, cancelled_at
, fulfillment_status
, orders.billing_address__country AS billing_order_country
, CASE WHEN customer_first_order.order_id IS NOT NULL THEN 'New' ELSE 'Returning' END AS order_type
, COUNT(orders.id) AS number_of_orders
, SUM(SAFE_CAST(subtotal_price AS FLOAT64)) AS subtotal_price
, SUM(SAFE_CAST(total_discounts AS FLOAT64)/(1+ifnull(tax_rate,0))) AS total_discounts
, SUM(SAFE_CAST(current_total_discounts AS FLOAT64)) AS current_total_discounts
, SUM(SAFE_CAST(total_line_items_price AS FLOAT64)) AS total_line_items_price
, SUM(IFNULL(SAFE_CAST(total_price AS FLOAT64),0)-IFNULL(SAFE_CAST(total_tax AS FLOAT64)-SAFE_CAST(shipping.price AS FLOAT64),0)+IFNULL(SAFE_CAST(total_discounts AS FLOAT64),0)) AS gross_sales_test
, SUM(order_line_agg.gross_sales) AS gross_sales
, SUM(order_line_agg.quantity) AS quantity
-- discount included the full discount amount, we need to remove the discount from free products, then take tax off the reamining. Free prodcts discount doesn't have any tax applied to it
, SUM((-IFNULL(SAFE_CAST(total_discounts AS FLOAT64),0)-IFNULL(free_product_discount.discount,0))/(1+ifnull(tax_rate,0))) AS discount
, SUM(free_product_discount.discount) AS free_product_discount
, SUM(-SAFE_CAST(current_total_discounts AS FLOAT64)) AS discount_v2
, SUM(SAFE_CAST(total_price AS FLOAT64)) AS total_sales
, SUM(SAFE_CAST(total_tax AS FLOAT64)) AS total_tax
, SUM(SAFE_CAST(current_total_tax AS FLOAT64)) AS current_total_tax
, SUM(CASE WHEN SAFE_CAST(total_tax AS FLOAT64) = 0 THEN SAFE_CAST(subtotal_price AS FLOAT64)-(SAFE_CAST(subtotal_price AS FLOAT64)/1.2) else SAFE_CAST(total_tax AS FLOAT64) END ) AS tax_v2
, SUM(SAFE_CAST(shipping.price AS FLOAT64)/(1+ifnull(tax_rate,0))) AS shipping_cost
, SUM(SAFE_CAST(shipping.price AS FLOAT64)) as shipping_cost_v2
, SUM(SAFE_CAST(duties AS FLOAT64)) AS duties
, SUM(IFNULL(SAFE_CAST(total_price AS FLOAT64),0)-IFNULL(SAFE_CAST(total_tax AS FLOAT64)-SAFE_CAST(shipping.price AS FLOAT64),0)) AS net_sales
FROM {{ref('raw_order_cleaned')}} orders
LEFT JOIN {{source('shopify', 'shipping_lines')}}  shipping
  ON orders.id = shipping.order_id
LEFT JOIN order_line_agg 
  ON orders.id = order_line_agg.order_id
LEFT JOIN {{ref('free_product_discount')}} free_product_discount
  ON orders.id = free_product_discount.order_id
-- # LEFT JOIN source('shopify', 'order_discount_codes') discount_code
--   ON orders.id = discount_code.order_id
LEFT JOIN {{ref('customer_first_order')}} customer_first_order
  ON orders.id = customer_first_order.order_id
WHERE orders.order_date >= '2024-01-01'
GROUP BY ALL
) ,

-- Getting the redeemed value of any orders using a giftcard
-- gift_card AS (

-- SELECT 
--   order_id
--   , sum(redeemed_value) AS gift_card_redeemed_value
--  FROM  ref('shopify_gift_card_uk')
--  GROUP BY 1


-- )  ,

-- creating the metrics using the shopify fields
stg_shopify_metrics AS (

SELECT
coalesce(orders.order_date,refunds_actual.refund_date) AS fulldate
, coalesce(orders.order_id,refunds_actual.order_id) AS order_id
, coalesce(orders.order_name,refunds_actual.order_name) AS order_name
, coalesce(orders.app_id,refunds_actual.app_id) AS app_id
, COALESCE(CAST(orders.user_id AS STRING), CAST(refunds_actual.user_id AS STRING)) AS user_id
, coalesce(orders.billing_order_country, refunds_actual.billing_order_country) AS billing_order_country
, orders.order_date
, orders.order_id AS orders_order_id
, orders.order_type AS order_type
, SHA256(CAST(TRIM(LOWER(orders.email)) AS STRING))  AS orders_email --making the email hashed
, orders.customer_id AS customer_id
, orders.user_id AS orders_user_id
, orders.total_sales AS original_total_sales
, orders.quantity AS quantity

-- refunds actuals returns on the date
, refunds_actual.refund_date
, refunds_actual.order_id AS ra_order_id
, refunds_actual.user_id AS ra_user_id
, refunds_actual.refund_amount_actuals 
-- , refunds_actual.refund_amount_before_order_adjustment
-- , ifnull(refunds_actual.refund_amount_actuals,0)  - ifnull(refunds_actual.refund_tax,0) as returns
, IFNULL(refunds_actual.refund_amount_actuals,0) AS returns

-- orders
, orders.number_of_orders AS number_of_orders
, refunds_actual.number_of_orders AS refund_order_count


-- Refund on the order placed
-- , orders.refund_subtotal_order_level
-- , orders.refund_total_tax_order_level
-- , orders.refund_amount_order_level

-- Taxes and Shipping and Duties
, IFNULL(SAFE_CAST(orders.total_tax AS FLOAT64),0)+IFNULL(refund_tax,0) AS tax_v2
, IFNULL(
    COALESCE(
      NULLIF(SAFE_CAST(orders.total_tax AS FLOAT64),0),
      NULLIF(SAFE_CAST(orders.current_total_tax AS FLOAT64),0)
    ),
    0
  )+IFNULL(refund_tax,0) AS tax --correct calculaton
, IFNULL(orders.duties,0) AS duties

, orders.shipping_cost AS shipping_cost_V2
, IFNULL(orders.shipping_cost,0)+IFNULL(refund_shipping_amount,0) AS shipping_cost --returning the shipping cost too

-- Discount
, orders.discount AS discountv2
-- , CASE WHEN code IN ('Custom discount','Gifting','BackOrder') THEN orders.discount ELSE orders.discount/1.2 END AS discountv3
-- , CASE WHEN code IN ('Custom discount') OR total_discounts = total_line_items_price THEN orders.discount ELSE ROUND(orders.discount/1.2,2) END AS discount_v4
, CASE WHEN current_total_discounts = total_line_items_price THEN orders.discount ELSE ROUND(orders.discount/1.2,2) END AS discount_v5
, IFNULL(orders.discount,0) + IFNULL(orders.free_product_discount,0) AS discount  -- correct calculation?
--Order Adjustment
-- , IFNULL(refunds_actual.order_adjustment_amount,0) AS order_adjustment_amount

--Order giftcard
-- , gift_card.order_id AS gift_card_order_id
-- , IFNULL(gift_card.gift_card_redeemed_value,0) AS gift_card_redeemed_value

-- Total Sales (Total Price)
, orders.total_sales AS total_sales_without_returns
, IFNULL(orders.total_sales,0) - IFNULL(refund_amount_actuals,0)  AS total_sales_v2
, IFNULL(orders.total_sales,0) + IFNULL(refund_amount_actuals,0) AS total_sales -- Correct for UK
-- , IFNULL(orders.total_sales,0) - 0 - IFNULL(gift_card_redeemed_value,0)   AS total_sales_v3

-- Gross Sales 
, orders.gross_sales AS gross_sales_without_returns
, orders.gross_sales - IFNULL(refund_amount_actuals,0)  AS gross_salesv2
, IFNULL(orders.total_sales,0) + IFNULL(refund_amount_actuals,0) --total_sales
  - IFNULL(orders.total_tax,0)+IFNULL(refund_tax,0) -- tax
  - IFNULL(orders.shipping_cost,0) -- shipping
  - IFNULL(orders.duties,0) -- duties
  + orders.discount -- gross_sales
  AS gross_sales

, gross_sales AS order_line_agg_gross_sales

-- Net Sales
, orders.net_sales net_salesv2
, IFNULL(orders.total_sales,0) + IFNULL(refund_amount_actuals,0) --total_sales
  - IFNULL(orders.total_tax,0)+ IFNULL(refund_tax,0) -- tax
  - IFNULL(orders.shipping_cost,0) -- shipping
  - IFNULL(orders.duties,0) -- duties
  AS net_sales

FROM orders_table AS orders
FULL JOIN {{ref('stg_refunds')}} AS refunds_actual -- refeunds based on the refund date
  ON orders.order_date = refunds_actual.refund_date
  AND orders.order_id = refunds_actual.order_id
-- LEFT JOIN gift_card
--   ON orders.order_id = gift_card.order_id 

) ,
-- There will a line per order id or returned order
shopify_metrics AS (
  SELECT
    fulldate
    , order_id
    , order_name
    , orders_order_id
    , stg_shopify_metrics.app_id
    --, app_id_definition.name AS app_name
    , ra_order_id AS return_order_id
    -- , gift_card_order_id
    , refund_date
    , user_id
    , orders_email
    , customer_id
    , order_type
    , billing_order_country
    , number_of_orders
    , quantity
    , refund_order_count
    , tax
    , shipping_cost
    , duties
    , discount
    , returns
    -- , gift_card_redeemed_value
    , total_sales
    , total_sales + ABS(returns) as total_sales_plus_returns
    , total_sales + IFNULL(ABS(returns),0) + IFNULL(ABS(discount),0) - tax-shipping_cost AS gross_sales
    , order_line_agg_gross_sales
    , total_sales - tax - shipping_cost as net_sales
    , total_sales - tax - shipping_cost + ABS(returns) AS net_sales_plus_returns
    , total_sales-tax-shipping_cost + shipping_cost AS sales
FROM stg_shopify_metrics
),
 -- Join with retail calendar
joined_data AS (
    SELECT
        shopify_metrics.*,                     -- All fields from your deduped fact table
        d.retail_year,           -- Include all retail calendar fields
        d.retail_week,
        d.retail_month,
        d.retail_quarter,
        d.retail_week_label,
        d.retail_month_name,
        d.retail_year_week_id,
        d.retail_year_month_id,
        d.retail_week_start_date,
        d.retail_week_end_date,
        d.day_of_week,
        d.day_name
        -- Add any other retail calendar fields you need
    FROM shopify_metrics
    LEFT JOIN {{ ref('retail_calendar') }} d ON shopify_metrics.fulldate = d.calendar_date
)
-- Final output
SELECT * FROM joined_data
