/*
CONTEXT:
- Creating a table at the order line level and with added product information
- Also flag whether that item was returned
RESULT EXPECTATION:
- Unique by order_line_id
ASSUMPTION:
-
*/
{{ 
    config(
        materialized='table',
        schema = 'staging'  
    ) 
}}

WITH order_line_product AS (
SELECT
  order_date
  , orders.billing_address__country AS billing_order_country
  , order_line.id AS order_line_id
  , order_line.order_id
  , order_line.product_id
  , order_line.variant_id
  , order_line.title
  -- , case when UPPER(TRIM(order_line.title)) = 'HYLO GIFT CARD'  then 'GIFTCARD' else order_line.sku end as sku 
  -- , dim_product.range -- range
  -- , dim_product.colour -- colour
  -- , dim_product.size -- size
  -- , dim_product.source -- source
  -- , dim_product.category -- product/garment
  -- , dim_product.location 
  , product.handle
  , product.product_type
  , product.vendor
  , REGEXP_EXTRACT(product.handle, r'[a-z]+') handle_first_string
  , tax_line.rate
  , order_line_refund.line_item_id AS refund_order_line_id
  , SUM(order_line_refund.quantity) AS refund_quantity 
  , SUM(order_line_refund.subtotal) AS refund_subtotal
  , SUM(SAFE_CAST(order_line.price AS FLOAT64)) AS total_sales
  , SUM(order_line.quantity) AS quantity
  , SUM(CASE 
          WHEN orders.billing_address__country NOT IN ('United Kingdom') THEN (SAFE_CAST(order_line.price AS FLOAT64)*order_line.quantity)
          WHEN tax_line.rate IS NOT NULL THEN (SAFE_CAST(order_line.price AS FLOAT64)*order_line.quantity)/(1+tax_line.rate)
          ELSE (SAFE_CAST(order_line.price AS FLOAT64)*order_line.quantity) 
    END) AS gross_sales
FROM {{source('shopify','line_items')}}  order_line --`hylo-data.hylo_fivetran_shopify.order_line`
INNER JOIN {{ref('raw_order_cleaned')}} orders
  ON order_line.order_id = orders.id
-- LEFT JOIN `hylo-data.prod.dim_product` dim_product
--   ON case when UPPER(TRIM(order_line.title)) = 'HYLO GIFT CARD'  then 'GIFTCARD' else order_line.sku end = dim_product.sku
--     AND UPPER(dim_product.location) = 'UK'
--   -- add uk to query
LEFT JOIN {{source('shopify','products')}} product
  ON order_line.product_id = product.id
LEFT JOIN {{source('shopify', 'refund_line_items')}} order_line_refund
  ON order_line.id = order_line_refund.line_item_id
LEFT JOIN {{ref('line_item_tax_lines')}} tax_line
  ON order_line.id = tax_line.order_line_id
  -- only taking the tax off sales price when it has GB VAT on it and the Price is not 0 - price is zero when full discount is applied example order_id = 35139106603393, order_line_id=35136208044417
  AND tax_line.title = 'GB VAT' AND tax_line.price != 0
-- where UPPER(order_line.title) not in ('HYLO GIFT CARD') 
GROUP BY ALL

)
SELECT
  order_date
  , billing_order_country
  , order_line_id
  , order_id
  , product_id
  , variant_id
  , title
  , handle
  , product_type
  , vendor
  , handle_first_string
  , rate
  , refund_order_line_id
  , refund_quantity 
  , refund_subtotal
  , total_sales
  , quantity
  , gross_sales
  , SUM(total_sales) OVER (PARTITION BY order_id) AS order_total
FROM order_line_product
GROUP BY ALL
