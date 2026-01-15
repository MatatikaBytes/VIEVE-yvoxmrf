
{{ 
    config(
        materialized='table',
        schema = 'staging'  
    ) 
}}

/*
CONTEXT:
- Flatten Shopify line_item.tax_lines so taxes are queryable without JSON parsing.
RESULT EXPECTATION:
- One row per tax line keyed by order_line_id.
*/

WITH line_item_tax_lines AS (
  SELECT
    order_line.id AS order_line_id,
    order_line.order_id,
    tax_line
  FROM {{ source('shopify','line_items') }} AS order_line
  -- tax_lines is an array on the line item; unnest directly
  CROSS JOIN UNNEST(IFNULL(JSON_EXTRACT_ARRAY(order_line.tax_lines, '$'), [])) AS tax_line
)
SELECT
  order_line_id,
  order_id,
  SAFE_CAST(JSON_VALUE(tax_line, '$.channel_agnostic') AS BOOL) AS channel_agnostic,
  JSON_VALUE(tax_line, '$.title') AS title,
  SAFE_CAST(JSON_VALUE(tax_line, '$.rate') AS FLOAT64) AS rate,
  SAFE_CAST(JSON_VALUE(tax_line, '$.rate_percentage') AS FLOAT64) AS rate_percentage,
  SAFE_CAST(JSON_VALUE(tax_line, '$.price') AS FLOAT64) AS price,
  JSON_QUERY(tax_line, '$.price_set') AS price_set -- keep price_set JSON intact
FROM line_item_tax_lines
