
{{ 
    config(
        materialized='table',
        schema = 'staging'  
    ) 
}}

WITH free_product AS (
    SELECT 
    id,
    order_id,
    -SAFE_CAST(price AS FLOAT64) AS discount,
    MAX(IF(
        JSON_EXTRACT_SCALAR(entry, '$.name') = '_yotpo_loyalty_discount_type' AND
        JSON_EXTRACT_SCALAR(entry, '$.value') = 'product',
        'product',
        NULL
    )) AS yotpo_discount_type_is_product
    FROM {{source('shopify','line_items')}} 
    , UNNEST(IFNULL(JSON_EXTRACT_ARRAY(properties, '$'), [])) AS entry
    GROUP BY 1,2,3
)

SELECT
    order_id,
    SUM(discount) AS discount
FROM free_product
WHERE yotpo_discount_type_is_product = 'product'
GROUP BY 1
