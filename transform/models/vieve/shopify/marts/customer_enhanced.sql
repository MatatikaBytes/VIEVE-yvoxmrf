/*
CONTEXT:
-- Customer segmentation with channel tracking (POS, Web, Marketplace)
-- Includes Magento historical orders with flag
-- Sweet Analytics style RFM + GM segmentation
-- Geographic enrichment for UK postcodes
-- LTV and AOV calculations
-- POS location tracking for Glasgow Pop-up and 2024 Roadshow events
-- NEW: customer_acquisition_location tracks where customer made FIRST ORDER EVER

RESULT EXPECTATION:
- Unique by email
- One row per customer
- Multi-channel behavior tracking
- Customer lifetime value metrics in GBP
- POS acquisition city (first POS order location)
- Customer acquisition location (first order ever location)

REVENUE FIELD ASSUMPTIONS:
- GROSS REVENUE: Uses shop_money from total_price_set (converted to GBP by Shopify)
- NET REVENUE: Uses shop_money from current_total_price_set (converted to GBP by Shopify)
- REFUNDS: Calculated as (gross_sales - net_sales)
- AOV: Calculated using NET revenue in GBP
- LTV: Total NET revenue per customer in GBP

DATA FILTERS:
- test = FALSE (excludes test orders)
- customer_id <> 23967718408577 (exclude erroneous Fivetran data)
- app_id NOT IN (1758145, 1354745) - Excludes Matrixify + Draft Orders from non-Magento source
- email IS NOT NULL
- financial_status IN ('paid', 'partially_paid', 'authorized', 'pending')
- shop_money.currency_code = 'GBP' (only orders properly converted to GBP)

APP_ID MAPPING (based on source_name analysis):
- 580111: Online Store (Web) - 154,784 orders
- 1758145: Matrixify App (Magento import) - 120,630 orders (handled separately)
- 129785: Point of Sale (POS) - 20,061 orders
- 1354745: Draft Orders (EXCLUDED) - 2,726 orders
- 4555605: TikTok Shop / AfterShip Feed - 1,491 orders
- 195233349633: Unknown Marketplace - 1,169 orders
- 3890849: Unknown channel - 150 orders
- 5806573: Unknown channel - 71 orders
- 4383523: TikTok - 4 orders

POS LOCATION ASSIGNMENT:
- pos_acquisition_city: Location of customer's FIRST POS order (app_id 129785)
  * May not match customer_acquisition_location if customer was acquired via web/magento first
- customer_acquisition_location: Location/channel of customer's FIRST ORDER EVER
  * Use this field for true acquisition analysis
- Roadshow events (2024):
  * London Roadshow: August 30, 2024
  * Birmingham Roadshow: September 1, 2024
  * Liverpool Roadshow: September 6, 2024
  * Manchester Roadshow: September 8, 2024
- Glasgow Pop-up: All other POS orders (permanent location, opened 2023)

CUSTOMER COHORTS:
- Pop-up + .com: Customers with both Shopify POS orders (app_id 129785) AND web orders (app_id 580111)
- .com only: Customers with web orders but NO Shopify POS orders
- Pop-up only: Customers with Shopify POS orders but NO web orders

ASSUMPTIONS:
1. LTV = Total net revenue per customer in GBP (historical spend, not predictive)
2. AOV = Average net order value in GBP (net_revenue / total_orders)
3. Customer age calculated from first order to current date
4. Geographic matching uses first part of UK postcode only
5. Multi-channel customers prioritized as 'Omnichannel' in channel_behavior
6. Acquisition channel determined by first order's app_id across ALL sources (including Magento)
7. POS location determined by first SHOPIFY POS order only (excludes Magento)
8. Customer acquisition location determined by first order's location (POS-specific for in-store)
9. TikTok orders from both app_id 4555605 and 4383523
10. Currency: Uses Shopify's shop_money field (already converted to GBP)
11. Roadshow dates use exact date matching

DATA QUALITY NOTES:
- pos_acquisition_city may differ from customer_acquisition_location for web/magento customers who later visited POS
- For true acquisition analysis, use customer_acquisition_location
- Roadshow sample sizes are small (100-170 customers per city acquired at events)
- 14+ months of post-roadshow data available (events occurred Aug-Sept 2024)
*/

-- TODO/IMPROVEMENTS:
-- PRIORITY: Model Validation & Testing
-- Create validation macro in dbt to compare against Shopify UI baseline

-- Model Improvements:
-- Consider adding location_id field from raw_order_cleaned to improve POS location assignment
-- Use date ranges instead of exact dates for roadshow assignment

-- Channel & Source Expansion:
-- Identify unknown app_ids: 195233349633, 3890849, 5806573
-- Separate TikTok from AfterShip Feed if needed for more granular analysis

{{ 
    config(
        materialized='table',
        schema = 'marts'  
    ) 
}}

WITH customer_orders AS (
    
    -- NON-MAGENTO ORDERS (already in GBP from Shopify)
    SELECT
        LOWER(email) AS email,
        order_date,
        CAST(total_line_items_price_set__shop_money__amount AS FLOAT64) AS gross_sales,
        CAST(current_total_price_set__shop_money__amount AS FLOAT64) AS net_sales,
        id AS order_id,
        customer__id as customer_id,
        app_id,
        source_name,
        billing_address__country as billing_address_country,
        billing_address__city as billing_address_city,
        billing_address__province as billing_address_province,
        billing_address__zip as billing_address_zip,
        shipping_address__country as shipping_address_country,
        shipping_address__city as shipping_address_city,
        shipping_address__province as shipping_address_province,
        shipping_address__zip as shipping_address_zip,
        buyer_accepts_marketing,
        FALSE AS is_magento_order,
        FALSE AS is_currency_converted
    FROM {{ref('raw_order_cleaned')}}
    WHERE test = FALSE
        AND customer__id <> 23967718408577
        AND app_id NOT IN (1758145, 1354745)
        AND email IS NOT NULL
        AND financial_status IN ('paid', 'partially_paid', 'authorized', 'pending')
        AND JSON_EXTRACT_SCALAR(total_price_set__shop_money__currency_code) = 'GBP'
    
    UNION ALL
    
    -- MAGENTO ORDERS (converted to GBP via clean_magento_orders staging table)
    SELECT
        LOWER(email) AS email,
        order_date,
        gross_sales_gbp AS gross_sales,
        net_sales_gbp AS net_sales,
        order_id,
        customer_id,
        app_id,
        source_name,
        billing_address_country,
        billing_address_city,
        billing_address_province,
        billing_address_zip,
        shipping_address_country,
        shipping_address_city,
        shipping_address_province,
        shipping_address_zip,
        buyer_accepts_marketing,
        TRUE AS is_magento_order,
        CASE WHEN conversion_status = 'CONVERTED_TO_GBP' THEN TRUE ELSE FALSE END AS is_currency_converted
    FROM {{ref('clean_magento_orders')}}
),

-- POS acquisition location: First POS order location
pos_acquisition_location AS (
  SELECT 
    LOWER(co.email) AS email,
    MIN(co.order_date) AS first_pos_order_date,
    CASE
      WHEN MIN(co.order_date) = '2024-08-30' THEN 'London Roadshow'
      WHEN MIN(co.order_date) = '2024-09-01' THEN 'Birmingham Roadshow'
      WHEN MIN(co.order_date) = '2024-09-06' THEN 'Liverpool Roadshow'
      WHEN MIN(co.order_date) = '2024-09-08' THEN 'Manchester Roadshow'
      ELSE 'Glasgow Pop-up'
    END AS pos_acquisition_city
  FROM customer_orders co
  WHERE co.app_id = 129785
  GROUP BY co.email
),

-- NEW: Customer acquisition location: First order EVER location
customer_acquisition_location AS (
  SELECT
    email,
    order_date AS first_order_date,
    app_id AS first_order_app_id,
    CASE
      -- Only assign specific location if FIRST order was POS
      WHEN app_id = 129785 THEN
        CASE
          WHEN order_date = '2024-08-30' THEN 'London Roadshow'
          WHEN order_date = '2024-09-01' THEN 'Birmingham Roadshow'
          WHEN order_date = '2024-09-06' THEN 'Liverpool Roadshow'
          WHEN order_date = '2024-09-08' THEN 'Manchester Roadshow'
          ELSE 'Glasgow Pop-up'
        END
      WHEN app_id = 580111 THEN 'Web'
      WHEN app_id = 1758145 THEN 'Magento'
      WHEN app_id IN (4555605, 4383523) THEN 'TikTok'
      WHEN app_id = 195233349633 THEN 'Marketplace'
      ELSE 'Other'
    END AS customer_acquisition_location,
    ROW_NUMBER() OVER (PARTITION BY email ORDER BY order_date ASC) AS rn
  FROM customer_orders
),

first_order_channel AS (
    SELECT
        email,
        CASE 
            WHEN app_id = 580111 THEN 'Web'
            WHEN app_id = 129785 THEN 'POS'
            WHEN app_id = 1758145 THEN 'Magento (TBC-confirmwithGiulia)'
            WHEN app_id IN (4555605, 4383523) THEN 'TikTok'
            WHEN app_id = 195233349633 THEN 'Marketplace'
            WHEN app_id IN (3890849, 5806573) THEN 'Other'
            ELSE 'Unknown'
        END AS acquisition_channel,
        CASE 
            WHEN app_id = 580111 THEN 'Online Store'
            WHEN app_id = 129785 THEN 'Point of Sale'
            WHEN app_id = 1758145 THEN 'Matrixify Import'
            WHEN app_id = 4555605 AND LOWER(source_name) LIKE '%tiktok%' THEN 'TikTok Shop'
            WHEN app_id = 4555605 AND LOWER(source_name) LIKE '%aftership%' THEN 'AfterShip Feed'
            WHEN app_id = 4555605 THEN 'TikTok/AfterShip'
            WHEN app_id = 4383523 THEN 'TikTok'
            WHEN app_id = 195233349633 THEN 'Marketplace (Unknown)'
            WHEN app_id = 3890849 THEN 'Unknown (3890849)'
            WHEN app_id = 5806573 THEN 'Unknown (5806573)'
            ELSE 'Other'
        END AS acquisition_channel_detail,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY order_date ASC) AS rn
    FROM customer_orders
),

customer_metrics AS (
    SELECT
        co.email
        
        -- Customer ID
        , MAX(co.customer_id) AS customer_id
        
        -- Platform flags
        , MAX(CASE WHEN co.app_id = 1758145 THEN TRUE ELSE FALSE END) AS has_magento_orders
        , COUNT(DISTINCT CASE WHEN co.app_id = 1758145 THEN co.order_id END) AS magento_orders
        , COUNT(DISTINCT CASE WHEN co.app_id != 1758145 THEN co.order_id END) AS shopify_orders
        
        -- Channel flags - Traditional
        , MAX(CASE WHEN co.app_id = 129785 THEN TRUE ELSE FALSE END) AS has_pos_orders
        , COUNT(DISTINCT CASE WHEN co.app_id = 129785 THEN co.order_id END) AS pos_orders
        
        , MAX(CASE WHEN co.app_id = 580111 THEN TRUE ELSE FALSE END) AS has_web_orders
        , COUNT(DISTINCT CASE WHEN co.app_id = 580111 THEN co.order_id END) AS web_orders
        
        -- Channel flags - Social Commerce
        , MAX(CASE WHEN co.app_id IN (4555605, 4383523) THEN TRUE ELSE FALSE END) AS has_tiktok_orders
        , COUNT(DISTINCT CASE WHEN co.app_id IN (4555605, 4383523) THEN co.order_id END) AS tiktok_orders
        
        -- Channel flags - Marketplace
        , MAX(CASE WHEN co.app_id IN (195233349633) THEN TRUE ELSE FALSE END) AS has_marketplace_orders
        , COUNT(DISTINCT CASE WHEN co.app_id IN (195233349633) THEN co.order_id END) AS marketplace_orders
        
        -- Channel flags - Unknown/Other
        , MAX(CASE WHEN co.app_id IN (3890849, 5806573) THEN TRUE ELSE FALSE END) AS has_other_channel_orders
        , COUNT(DISTINCT CASE WHEN co.app_id IN (3890849, 5806573) THEN co.order_id END) AS other_channel_orders
        
        -- Acquisition channel (first order)
        , MAX(foc.acquisition_channel) AS acquisition_channel
        , MAX(foc.acquisition_channel_detail) AS acquisition_channel_detail
        
        -- NEW: Customer acquisition location (first order ever)
        , MAX(cal.customer_acquisition_location) AS customer_acquisition_location
        
        -- Addresses
        , MAX(co.billing_address_country) AS billing_order_country
        , MAX(co.billing_address_city) AS billing_city
        , MAX(co.billing_address_province) AS billing_province
        , MAX(co.billing_address_zip) AS billing_postcode
        , MAX(co.shipping_address_country) AS shipping_order_country
        , MAX(co.shipping_address_city) AS shipping_city
        , MAX(co.shipping_address_province) AS shipping_province
        , MAX(co.shipping_address_zip) AS shipping_postcode
        
        , APPROX_TOP_COUNT(co.source_name, 1)[OFFSET(0)].value AS primary_order_source
        , MAX(co.buyer_accepts_marketing) AS accepts_marketing
        
        -- Date metrics
        , MIN(co.order_date) AS first_order_date
        , MAX(co.order_date) AS last_order_date
        , DATE_DIFF(CURRENT_DATE(), MAX(co.order_date), DAY) AS days_since_last_order
        , DATE_DIFF(CURRENT_DATE(), MIN(co.order_date), DAY) AS customer_age_days
        
        -- Order counts
        , COUNT(DISTINCT co.order_id) AS total_orders
        , COUNT(DISTINCT CASE 
            WHEN co.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
            THEN co.order_id 
          END) AS orders_last_90_days
        , COUNT(DISTINCT CASE 
            WHEN co.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
            THEN co.order_id 
          END) AS orders_last_180_days
        , COUNT(DISTINCT CASE 
            WHEN co.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
            THEN co.order_id 
          END) AS orders_last_365_days
        
        -- Revenue metrics - GROSS (in GBP)
        , SUM(co.gross_sales) AS gross_revenue
        , AVG(co.gross_sales) AS avg_gross_order_value
        , MAX(co.gross_sales) AS max_gross_order_value
        
        -- Revenue metrics - NET (in GBP)
        , SUM(co.net_sales) AS net_revenue
        , AVG(co.net_sales) AS avg_net_order_value
        , MAX(co.net_sales) AS max_net_order_value
        
        -- Revenue by channel - for analysis (in GBP)
        , SUM(CASE WHEN co.app_id = 580111 THEN co.net_sales ELSE 0 END) AS web_revenue
        , SUM(CASE WHEN co.app_id = 129785 THEN co.net_sales ELSE 0 END) AS pos_revenue
        , SUM(CASE WHEN co.app_id IN (4555605, 4383523) THEN co.net_sales ELSE 0 END) AS tiktok_revenue
        , SUM(CASE WHEN co.app_id = 195233349633 THEN co.net_sales ELSE 0 END) AS marketplace_revenue
        
        -- Refund metrics
        , SUM(co.gross_sales - co.net_sales) AS total_refunded
        , ROUND(SUM(co.gross_sales - co.net_sales) * 100.0 / NULLIF(SUM(co.gross_sales), 0), 2) AS refund_rate_pct

        -- POS acquisition location (first POS order)
        , MAX(pal.pos_acquisition_city) AS pos_acquisition_city
        , MAX(pal.first_pos_order_date) AS first_pos_order_date
        
    FROM customer_orders co
    LEFT JOIN first_order_channel foc 
        ON co.email = foc.email 
        AND foc.rn = 1
    LEFT JOIN customer_acquisition_location cal
        ON co.email = cal.email
        AND cal.rn = 1
    LEFT JOIN pos_acquisition_location pal
        ON co.email = pal.email 
    GROUP BY co.email
),

/*dim_postal_codes AS (
    SELECT 
        Postcode AS postcode,
        Latitude AS latitude,
        Longitude AS longitude,
        Population AS population,
        Households AS households,
        `Nearby districts` AS nearby_districts,
        `UK region` AS uk_region
    FROM {{ source('shopify', 'dim_postcode_uk') }}
),*/

customer_segmentation AS (
    SELECT
        cm.email
        , cm.customer_id
        
        -- Platform flags
        , cm.has_magento_orders
        , cm.magento_orders
        , cm.shopify_orders
        
        -- Channel flags
        , cm.has_pos_orders
        , cm.pos_orders
        , cm.has_web_orders
        , cm.web_orders
        , cm.has_tiktok_orders
        , cm.tiktok_orders
        , cm.has_marketplace_orders
        , cm.marketplace_orders
        , cm.has_other_channel_orders
        , cm.other_channel_orders
        
        -- Acquisition
        , cm.acquisition_channel
        , cm.acquisition_channel_detail
        , cm.customer_acquisition_location  -- NEW: True acquisition location

        -- POS location tracking
        , cm.pos_acquisition_city
        , cm.first_pos_order_date
        
        -- Channel behavior
        , CASE
            WHEN (CASE WHEN cm.has_web_orders THEN 1 ELSE 0 END +
                  CASE WHEN cm.has_pos_orders THEN 1 ELSE 0 END +
                  CASE WHEN cm.has_tiktok_orders THEN 1 ELSE 0 END +
                  CASE WHEN cm.has_marketplace_orders THEN 1 ELSE 0 END) >= 2 
                THEN 'Omnichannel'
            WHEN cm.has_web_orders AND NOT (cm.has_pos_orders OR cm.has_tiktok_orders OR cm.has_marketplace_orders) 
                THEN 'Online Only'
            WHEN cm.has_pos_orders AND NOT (cm.has_web_orders OR cm.has_tiktok_orders OR cm.has_marketplace_orders) 
                THEN 'In-Store Only'
            WHEN cm.has_tiktok_orders AND NOT (cm.has_web_orders OR cm.has_pos_orders OR cm.has_marketplace_orders) 
                THEN 'TikTok Only'
            WHEN cm.has_marketplace_orders AND NOT (cm.has_web_orders OR cm.has_pos_orders OR cm.has_tiktok_orders) 
                THEN 'Marketplace Only'
            ELSE 'Other'
          END AS channel_behavior
        
        -- Channel count
        , (CASE WHEN cm.has_web_orders THEN 1 ELSE 0 END +
           CASE WHEN cm.has_pos_orders THEN 1 ELSE 0 END +
           CASE WHEN cm.has_tiktok_orders THEN 1 ELSE 0 END +
           CASE WHEN cm.has_marketplace_orders THEN 1 ELSE 0 END) AS unique_channels_used
        
        -- Revenue by channel
        , cm.web_revenue
        , cm.pos_revenue
        , cm.tiktok_revenue
        , cm.marketplace_revenue
        
        -- Dominant channel (by revenue)
        , CASE
            WHEN cm.web_revenue >= GREATEST(cm.pos_revenue, cm.tiktok_revenue, cm.marketplace_revenue) THEN 'Web'
            WHEN cm.pos_revenue >= GREATEST(cm.web_revenue, cm.tiktok_revenue, cm.marketplace_revenue) THEN 'POS'
            WHEN cm.tiktok_revenue >= GREATEST(cm.web_revenue, cm.pos_revenue, cm.marketplace_revenue) THEN 'TikTok'
            WHEN cm.marketplace_revenue > 0 THEN 'Marketplace'
            ELSE 'Other'
          END AS dominant_channel_by_revenue
        
        -- Addresses
        , cm.billing_order_country
        , cm.billing_city
        , cm.billing_province
        , cm.billing_postcode
        , cm.shipping_order_country
        , cm.shipping_city
        , cm.shipping_province
        , cm.shipping_postcode
        
        -- Extract postal area
        , REGEXP_EXTRACT(cm.shipping_postcode, '^([A-Z]{1,2}[0-9]{1,2}[A-Z]?)') AS shipping_postal_area
        
        -- Geographic enrichment
        /*
        , geo.latitude AS shipping_latitude
        , geo.longitude AS shipping_longitude
        , geo.population AS area_population
        , geo.households AS area_households
        , geo.nearby_districts
        , COALESCE(geo.uk_region, 'Unknown') AS uk_region
        
        -- Data quality flag
        , CASE WHEN geo.postcode IS NULL AND cm.shipping_order_country = 'GB' THEN TRUE ELSE FALSE END AS is_postcode_unmatched*/
        
        , cm.primary_order_source
        , cm.accepts_marketing
        , cm.first_order_date
        , cm.last_order_date
        , cm.days_since_last_order
        , cm.customer_age_days
        
        -- Order metrics
        , cm.total_orders
        , cm.orders_last_90_days
        , cm.orders_last_180_days
        , cm.orders_last_365_days
        
        -- Revenue metrics - GROSS
        , cm.gross_revenue
        , cm.avg_gross_order_value
        , cm.max_gross_order_value
        
        -- Revenue metrics - NET
        , cm.net_revenue
        , cm.avg_net_order_value
        , cm.max_net_order_value
        
        -- LTV & AOV
        , ROUND(cm.net_revenue, 2) AS customer_lifetime_value
        , ROUND(cm.net_revenue / NULLIF(cm.total_orders, 0), 2) AS average_order_value
        
        -- Additional value metrics
        , ROUND(cm.net_revenue / NULLIF(cm.customer_age_days, 0) * 365, 2) AS annualized_value
        , ROUND(cm.net_revenue / NULLIF(cm.customer_age_days, 0) * 30, 2) AS avg_monthly_value
        
        -- Refund metrics
        , cm.total_refunded
        , cm.refund_rate_pct
        
        -- Customer type
        , CASE 
            WHEN cm.total_orders > 1 THEN 'Returning customer' 
            ELSE 'New customer' 
          END AS customer_type
        
        -- RFM Segment
        , CASE 
            WHEN cm.days_since_last_order < 90 AND cm.total_orders >= 4 
                THEN 'High Value Recent Customers'
            WHEN cm.days_since_last_order < 90 AND cm.total_orders BETWEEN 1 AND 3 
                THEN 'Recent Customers'
            WHEN cm.days_since_last_order BETWEEN 90 AND 269 AND cm.total_orders >= 4 
                THEN 'High Value Customer'
            WHEN cm.days_since_last_order BETWEEN 90 AND 269 AND cm.total_orders BETWEEN 2 AND 3 
                THEN 'Potential High Value'
            WHEN cm.days_since_last_order BETWEEN 90 AND 269 AND cm.total_orders = 1 
                THEN 'First Time Customers'
            WHEN cm.days_since_last_order BETWEEN 270 AND 364 AND cm.total_orders >= 4 
                THEN 'High Value Lapsing'
            WHEN cm.days_since_last_order BETWEEN 270 AND 364 AND cm.total_orders BETWEEN 1 AND 3 
                THEN 'Lapsing'
            WHEN cm.days_since_last_order >= 365 AND cm.total_orders >= 4 
                THEN 'High Value Lapsed'
            WHEN cm.days_since_last_order >= 365 AND cm.total_orders BETWEEN 2 AND 3 
                THEN 'Lapsed'
            WHEN cm.days_since_last_order >= 365 AND cm.total_orders = 1 
                THEN 'Lapsed One Time'
            ELSE 'Uncategorised'
          END AS rfm_segment
        
        -- GM Segment
        , CASE
            WHEN EXTRACT(YEAR FROM cm.first_order_date) = EXTRACT(YEAR FROM CURRENT_DATE())
              THEN 'NEW'
            WHEN EXTRACT(YEAR FROM cm.first_order_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
              THEN 'PYNEW'
            WHEN cm.orders_last_365_days > 0 
              AND EXTRACT(YEAR FROM cm.first_order_date) < EXTRACT(YEAR FROM CURRENT_DATE()) - 1
              THEN 'PYNON'
            WHEN cm.days_since_last_order BETWEEN 365 AND 729
              THEN 'PY1INACT'
            WHEN cm.days_since_last_order BETWEEN 730 AND 1094
              THEN 'PY2INACT'
            WHEN cm.days_since_last_order >= 1095
              THEN 'PY3INACT'
            ELSE 'Uncategorised'
          END AS gm_segment
        
        -- Recency category
        , CASE 
            WHEN cm.days_since_last_order < 90 THEN 'Recent (0-3 months)'
            WHEN cm.days_since_last_order < 270 THEN 'Active (3-9 months)'
            WHEN cm.days_since_last_order < 365 THEN 'Lapsing (9-12 months)'
            ELSE 'Lapsed (12+ months)'
          END AS recency_category
        
        -- Frequency category
        , CASE 
            WHEN cm.total_orders >= 4 THEN 'High Value (4+ orders)'
            WHEN cm.total_orders = 3 THEN 'Repeat (3 orders)'
            WHEN cm.total_orders = 2 THEN 'Repeat (2 orders)'
            WHEN cm.total_orders = 1 THEN 'One-time'
            ELSE 'No orders'
          END AS frequency_category
        
        -- Monetary tier
        , CASE 
            WHEN cm.net_revenue >= 500 THEN 'Tier 1 (£500+)'
            WHEN cm.net_revenue >= 200 THEN 'Tier 2 (£200-499)'
            WHEN cm.net_revenue >= 100 THEN 'Tier 3 (£100-199)'
            WHEN cm.net_revenue >= 50 THEN 'Tier 4 (£50-99)'
            WHEN cm.net_revenue > 0 THEN 'Tier 5 (<£50)'
            ELSE 'No purchases'
          END AS monetary_tier
        
        -- LTV tier
        , CASE 
            WHEN cm.net_revenue >= 1000 THEN 'VIP (£1000+)'
            WHEN cm.net_revenue >= 500 THEN 'High Value (£500-999)'
            WHEN cm.net_revenue >= 200 THEN 'Medium Value (£200-499)'
            WHEN cm.net_revenue >= 100 THEN 'Standard (£100-199)'
            WHEN cm.net_revenue > 0 THEN 'Low Value (<£100)'
            ELSE 'No purchases'
          END AS ltv_tier
        
    FROM customer_metrics cm
    /*LEFT JOIN dim_postal_codes geo
        ON REGEXP_EXTRACT(cm.shipping_postcode, '^([A-Z]{1,2}[0-9]{1,2}[A-Z]?)') = geo.postcode*/
)

SELECT * FROM customer_segmentation