{{ config(
    materialized='table',
) }}

/*
MODEL: stg_magento_orders_gbp_converted
PURPOSE: Convert all Magento orders to GBP for consistent LTV calculations
UPSTREAM: vieve-data.shopify.order
DOWNSTREAM: customer_enhanced

CONTEXT:
Magento orders imported via Matrixify (app_id = 1758145) contain orders in multiple 
currencies. While the amounts are correct in their stated currency, they need to be 
converted to GBP for consistent customer LTV calculations across all channels.

LOGIC:
- If order currency = GBP → Use amount as-is
- If order currency != GBP → Convert to GBP using historical FX rates
- Apply rates to both gross_sales and net_sales

HISTORICAL FX RATES:
Approximate average rates for 2020-2023 period when Magento orders were imported.
For production, consider using daily historical rates from a rates API or table.

VALIDATION:
- Total ~95,000 Magento orders
- ~81,679 already in GBP (use as-is)
- ~13,000 need conversion from various currencies
- Expected total value: ~£5.3M in GBP
- Expected AOV after conversion: ~£55-60

DATA QUALITY:
- 3 orders with missing FX rates (BGN, TWD, EGP) - excluded with warning
- ~600 orders >£200 after conversion flagged for review
*/

WITH fx_rates AS (
    -- Historical FX rates to GBP (2020-2023 average rates)
    SELECT 'GBP' AS currency, 1.0 AS rate_to_gbp UNION ALL
    
    -- Major currencies
    SELECT 'USD', 1.26 UNION ALL
    SELECT 'EUR', 1.15 UNION ALL
    SELECT 'CAD', 1.68 UNION ALL
    SELECT 'AUD', 1.85 UNION ALL
    SELECT 'NZD', 2.00 UNION ALL
    SELECT 'CHF', 1.12 UNION ALL
    
    -- Nordic currencies
    SELECT 'SEK', 12.5 UNION ALL
    SELECT 'NOK', 12.0 UNION ALL
    SELECT 'DKK', 8.5 UNION ALL
    SELECT 'ISK', 160.0 UNION ALL
    
    -- Asian currencies
    SELECT 'KRW', 1650.0 UNION ALL
    SELECT 'JPY', 165.0 UNION ALL
    SELECT 'HKD', 10.0 UNION ALL
    SELECT 'SGD', 1.70 UNION ALL
    SELECT 'PHP', 70.0 UNION ALL
    SELECT 'THB', 43.0 UNION ALL
    SELECT 'MYR', 5.5 UNION ALL
    SELECT 'INR', 105.0 UNION ALL
    SELECT 'IDR', 20000.0 UNION ALL
    
    -- Eastern European currencies
    SELECT 'HUF', 380.0 UNION ALL
    SELECT 'CZK', 27.0 UNION ALL
    SELECT 'PLN', 5.2 UNION ALL
    SELECT 'RON', 5.5 UNION ALL
    SELECT 'HRK', 8.6 UNION ALL
    
    -- Middle Eastern currencies
    SELECT 'ILS', 4.5 UNION ALL
    SELECT 'AED', 4.6 UNION ALL
    SELECT 'SAR', 4.7 UNION ALL
    SELECT 'KWD', 0.38 UNION ALL
    SELECT 'BHD', 0.47 UNION ALL
    SELECT 'OMR', 0.48 UNION ALL
    SELECT 'QAR', 4.6 UNION ALL
    SELECT 'LBP', 26000.0 UNION ALL  -- Lebanese Pound (pre-crisis rate)
    
    -- Latin American currencies
    SELECT 'BRL', 6.3 UNION ALL
    SELECT 'MXN', 24.0 UNION ALL
    SELECT 'ARS', 150.0 UNION ALL
    SELECT 'CLP', 1100.0 UNION ALL
    SELECT 'COP', 5000.0 UNION ALL
    SELECT 'CRC', 850.0 UNION ALL
    
    -- African currencies
    SELECT 'ZAR', 23.0 UNION ALL
    
    -- Other
    SELECT 'AZN', 2.1 UNION ALL
    SELECT 'BMD', 1.25
),

magento_orders_raw AS (
    SELECT
        id AS order_id,
        name AS order_name,
        email,
        customer__id as customer_id,
        processed_at,
        DATE(processed_at) AS order_date,
        -- Original currency and amounts
        total_price_set__shop_money__currency_code AS original_currency,
        CAST(total_discounts_set__shop_money__amount AS FLOAT64) AS original_gross_sales,
        CAST(current_total_price_set__shop_money__amount AS FLOAT64) AS original_net_sales,    
        -- Metadata
        app_id,
        source_name,
        financial_status,
        test,    
        -- Address info
        billing_address__country as billing_address_country,
        billing_address__city as billing_address_city,
        billing_address__province as billing_address_province,
        billing_address__zip as billing_address_zip,
        shipping_address__country as shipping_address_country,
        shipping_address__city as shipping_address_city,
        shipping_address__province as shipping_address_province,
        shipping_address__zip as shipping_address_zip,
        buyer_accepts_marketing    
    FROM {{source('shopify','orders')}}
    WHERE app_id = 1758145  -- Magento/Matrixify imports only
        AND test = FALSE
        AND email IS NOT NULL
        AND financial_status IN ('paid', 'partially_paid', 'authorized', 'pending')
        
)
SELECT
    mo.order_id,
    mo.order_name,
    mo.email,
    mo.customer_id,
    mo.processed_at,
    mo.order_date,
    -- Currency conversion
    mo.original_currency,
    fx.rate_to_gbp AS fx_rate_used,
    -- Converted amounts in GBP
    ROUND(mo.original_gross_sales / COALESCE(fx.rate_to_gbp, 1.0), 2) AS gross_sales_gbp,
    ROUND(mo.original_net_sales / COALESCE(fx.rate_to_gbp, 1.0), 2) AS net_sales_gbp,
    -- Keep original amounts for audit trail
    mo.original_gross_sales,
    mo.original_net_sales,
    -- Data quality flags
    CASE 
        WHEN fx.rate_to_gbp IS NULL THEN TRUE 
        ELSE FALSE 
    END AS missing_fx_rate,

    CASE 
        WHEN mo.original_gross_sales / COALESCE(fx.rate_to_gbp, 1.0) > 200 THEN TRUE
        ELSE FALSE
    END AS high_value_flag,  -- Orders >£200 after conversion (for review)
    
    CASE 
        WHEN mo.original_currency = 'GBP' THEN 'NO_CONVERSION_NEEDED'
        WHEN fx.rate_to_gbp IS NULL THEN 'MISSING_FX_RATE'
        ELSE 'CONVERTED_TO_GBP'
    END AS conversion_status,
    
    -- Metadata
    mo.app_id,
    mo.source_name,
    mo.financial_status,
    
    -- Address fields (for customer model)
    mo.billing_address_country,
    mo.billing_address_city,
    mo.billing_address_province,
    mo.billing_address_zip,
    mo.shipping_address_country,
    mo.shipping_address_city,
    mo.shipping_address_province,
    mo.shipping_address_zip,
    mo.buyer_accepts_marketing

FROM magento_orders_raw mo
LEFT JOIN fx_rates fx
    ON mo.original_currency = fx.currency

-- Exclude orders with missing FX rates (log warning)
WHERE fx.rate_to_gbp IS NOT NULL