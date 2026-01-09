/*
CONTEXT:
- Creating a the refunds actual table, this is creating a table order id and the date the refund occured a refund can be across multiple dates
- 
RESULT EXPECTATION:
- Unique by order_id, order_daye and refund_date, it is posisble an order can hae multiple return dates
- The original order id should not be present in the data if an item was exchaged in that order
ASSUMPTION:
- 
EXTRA NOTES
- the original exchanged order date is included for returns might not be fit for purpose
*/

{{ 
    config(
        materialized='table',
        schema = 'matatika_shopify_staging'  
    ) 
}}
-- Grouping by refund id getting the order details
WITH refund_orderline AS 
(
  SELECT
    id as refund_id
    , SUM(order_line_product.quantity) AS quantity
    , SUM(SAFE_CAST(subtotal AS FLOAT64)) AS subtotal
    , SUM(SAFE_CAST(total_tax AS FLOAT64)) AS total_tax
    , SUM(order_line_product.total_sales/NULLIF(order_line_product.order_total,0)) AS order_sales_return_rate
    FROM {{source('shopify', 'refund_line_items')}} AS order_line_refund
  LEFT JOIN {{ref('order_line_product')}} order_line_product
      ON order_line_refund.line_item_id = order_line_product.order_line_id
  GROUP BY 1
) ,

-- Using the order adjustment table that corrects the order info 
-- Adjusting the ammount just for shipping refunds

order_adjustment_table_clean AS (

  SELECT
    refund_id
    , _sdc_extracted_at
    , kind
    , reason
    , SAFE_CAST(amount AS FLOAT64) AS amount
    , SAFE_CAST(tax_amount AS FLOAT64) AS tax_amount
  FROM {{source('shopify', 'order_adjustments')}}
--   WHERE reason not like 'Pending%'
  -- chosing one refund_id as there are duplicates in the order adjustment table
  -- QUALIFY ROW_NUMBER() OVER (PARTITION BY refund_id, kind  ORDER BY id desc) = 1 
) ,

order_adjustment_table AS (
  -- making sure all order adustments are absolute values so we can net represent them later as a deduction, there are some cases where amount field is negative and postive it looks like it should always be a deduction
  -- when the order adjustment is related to refund_discrepancy it's represents the full refund amound including shipping and taxes
  -- when the other adjustment is related to shipping_refund it's the shipping amount and shipping tax amount of the order
  SELECT
    refund_id
    , SUM(CASE WHEN kind = 'shipping_refund' THEN ifnull(amount,0) ELSE NULL END) AS order_adjustment_shipping_amount
    , SUM(CASE WHEN kind = 'shipping_refund' THEN ifnull(tax_amount,0) ELSE NULL END) AS order_adjustment_shipping_tax
    , SUM(CASE WHEN kind = 'refund_discrepancy'
            THEN ifnull(amount,0) ELSE NULL END) AS order_adjustment_refund_discrepancy_amount
    , SUM(CASE WHEN kind = 'refund_discrepancy' 
          THEN ifnull(tax_amount,0) ELSE NULL END) AS order_adjustment_refund_discrepancy_tax
    , SUM(0) AS quantity
    , SUM(amount) AS amount
    , SUM(tax_amount) AS tax_amount
  FROM order_adjustment_table_clean
  GROUP BY 1
) ,

-- From the source refund table getting all the relevant metrics
-- Metrics in negative from -100 refund for example
stg_refund_actuals_table AS (
SELECT
  refund.*
  , refund.id AS refund_id
  , CAST(DATETIME(SAFE_CAST(refund.created_at AS TIMESTAMP), "Europe/London") AS DATE) AS refund_date
  , CAST(DATETIME(SAFE_CAST(orders.created_at AS TIMESTAMP), "Europe/London") AS DATE) AS order_date
  , orders.app_id
  , orders.name AS order_name
  , orders.presentment_currency
  -- , order_adjustment.reason as order_adjustment_reason
  , orders.billing_address__country AS billing_order_country
--   , SUM(order_line_refund.order_sales_return_rate) AS order_sales_return_rate
  , SUM(IFNULL(order_line_refund.subtotal,0)) AS subtotal
  , SUM(IFNULL(order_line_refund.total_tax,0)) AS total_tax
  , SUM(IFNULL(order_line_refund.quantity,0)) AS quantity
--   , SUM(IFNULL(-order_line_refund.subtotal,0)) refund_amount_before_order_adjustment
  -- taking the refund discrepancy amount into account first if it's null then we take the orderline refund subtotal and order shipping amount + tax
FROM {{source('shopify', 'refunds')}} refund
LEFT JOIN refund_orderline order_line_refund
  ON order_line_refund.refund_id = refund.id
LEFT JOIN {{source('shopify', 'orders')}} orders 
  ON orders.id = refund.order_id
GROUP BY ALL
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY refund.order_id, processed_at ORDER BY order_adjustment.refund_id desc) = 1 -- could be refundant
) ,


-- Grouping by the necessary fields
refund_actuals_table AS (
SELECT
  refund_date 
  , order_date
  , order_name
  , user_id
  , order_id
  , app_id
  , billing_order_country
  , presentment_currency
--   , SUM(order_sales_return_rate) AS order_sales_return_rate
  , SUM(stg_refund_actuals_table.quantity) AS quantity
  , COUNT(DISTINCT order_id) AS number_of_orders --multiple return dates for a single order be careful if you aggregate
  , SUM(subtotal) AS subtotal
  , SUM(total_tax) AS total_tax
  , SUM(order_adjustment_shipping_amount) AS order_adjustment_shipping_amount
  , SUM(order_adjustment_shipping_tax) AS order_adjustment_shipping_tax
  , SUM(order_adjustment_refund_discrepancy_amount) AS order_adjustment_refund_discrepancy_amount
  , SUM(order_adjustment_refund_discrepancy_tax) AS order_adjustment_refund_discrepancy_tax
FROM stg_refund_actuals_table
LEFT JOIN order_adjustment_table AS order_adjustment
  ON stg_refund_actuals_table.refund_id = order_adjustment.refund_id
GROUP BY ALL

) 

SELECT
    refund_date 
    , order_date
    , order_id
    , order_name
    , user_id
    , app_id
    , billing_order_country
    , presentment_currency
    , SUM(quantity) AS quantity
    , COUNT(DISTINCT order_id) AS number_of_orders --multiple return dates for a single order be careful if you aggregate
    , SUM(subtotal) AS subtotal
    , SUM(total_tax) AS total_tax
    --  Need check signs
    , SUM(IFNULL(-total_tax,0) + IFNULL(order_adjustment_refund_discrepancy_tax,0) + IFNULL(order_adjustment_shipping_tax,0)) AS refund_tax
    , SUM(order_adjustment_shipping_amount) AS order_adjustment_shipping_amount
     --  Need check signs
    , SUM(order_adjustment_shipping_amount) AS refund_shipping_amount
    , SUM(order_adjustment_shipping_tax) AS order_adjustment_shipping_tax
    , SUM(order_adjustment_refund_discrepancy_amount) AS order_adjustment_refund_discrepancy_amount
    , SUM(order_adjustment_refund_discrepancy_tax) AS order_adjustment_refund_discrepancy_tax
    ,  SUM(
        ROUND(
        CASE 
          WHEN presentment_currency IN ('GBP') 
              THEN (IFNULL(-subtotal,0)
                      + IFNULL(total_tax,0)
                      + IFNULL(order_adjustment_refund_discrepancy_amount,0)
                      -- + IFNULL(order_adjustment_shipping_amount,0)
                      -- + IFNULL(order_adjustment_shipping_tax,0)
                      )
          WHEN  presentment_currency NOT IN ('GBP')
              THEN (IFNULL(-subtotal,0)
                      -- - (IFNULL(total_tax,0))
                      + IFNULL(order_adjustment_refund_discrepancy_amount,0)
                      -- + IFNULL(order_adjustment_shipping_amount,0)
                      -- + IFNULL(order_adjustment_shipping_tax,0)
                      + (IFNULL(order_adjustment_refund_discrepancy_tax,0))
                      )        
          ELSE NULL
        END,2)
    ) AS refund_amount_actuals
FROM refund_actuals_table
GROUP BY ALL
