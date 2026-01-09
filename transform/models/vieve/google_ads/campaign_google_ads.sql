{{ 
    config(
        materialized='table',
        schema = 'matatika_google_ads_staging'  
    ) 
}}

/*
CONTEXT:
- Creates a unified Google Ads table that properly handles the hierarchy
- Uses campaign-level data as the base since it has the complete spend picture
- Enriches with ad group and keyword details where available
- Avoids duplication by aggregating before joining
*/

WITH 
-- Start with campaign data as the base (this has all spend)
campaign_base AS (
    SELECT 
        cs.segments__date as fulldate
        , cs.customer__id as account_id
        , cs.campaign__id as campaign_id
        , cs.segments__device as device
        , cs.metrics__cost_micros/1000000.0 as cost
        , cs.metrics__impressions as impressions
        , cs.metrics__clicks as clicks
        , cs.metrics__conversions as conversions
        , cs.metrics__conversions_value as conversions_value
        , 'campaign' as data_level
    FROM {{ source('google_ads', 'campaign_stats') }} cs
),

-- Get campaign metadata
campaign_info AS (
    SELECT 
        ch.customer__id as customer_id
        , ch.campaign__id as campaign_id
        , ch.campaign__name as campaign_name
        , ch.campaign__status as latest_status
        , ch.campaign__advertising_channel_type as advertising_channel_type
    FROM {{ source('google_ads', 'campaign_history') }} ch
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ch.customer__id, ch.campaign__id ORDER BY ch._sdc_extracted_at DESC) = 1
),

-- Aggregate ad group data to campaign level for comparison
ad_group_summary AS (
    SELECT 
        ags.segments__date as date
        , ags.customer__id as customer_id
        , ags.campaign__id as campaign_id
        , SUM(ags.metrics__cost_micros/1000000.0) as ad_group_cost
        , SUM(ags.metrics__impressions) as ad_group_impressions
        , SUM(ags.metrics__clicks) as ad_group_clicks
        , SUM(ags.metrics__conversions) as ad_group_conversions
        , SUM(ags.metrics__conversions_value) as ad_group_conversions_value
        , COUNT(DISTINCT ags.ad_group__id) as ad_group_count
    FROM {{ source('google_ads', 'ad_group_stats') }} ags
    GROUP BY 1, 2, 3
),

-- Aggregate keyword data to campaign level for comparison
keyword_summary AS (
    SELECT 
        ks.segments__date as date
        , ks.customer__id as customer_id
        , ks.campaign__id as campaign_id
        , SUM(ks.metrics__cost_micros/1000000.0) as keyword_cost
        , SUM(ks.metrics__impressions) as keyword_impressions
        , SUM(ks.metrics__clicks) as keyword_clicks
        , SUM(ks.metrics__conversions) as keyword_conversions
        , SUM(ks.metrics__conversions_value) as keyword_conversions_value
        ,COUNT(DISTINCT CONCAT(ks.ad_group__id, '-', ks.ad_group_criterion__criterion_id)) as keyword_count
    FROM {{ source('google_ads', 'keyword_stats') }} ks
    GROUP BY 1, 2, 3
),

-- Join everything together, using campaign as the base
unified AS (
    SELECT 
        cb.fulldate,
        cb.account_id,
        cb.campaign_id,
        ci.campaign_name,
        ci.latest_status,
        ci.advertising_channel_type,
        cb.device,
        -- Always use campaign-level cost (most complete)
        cb.cost,
        -- Use the most detailed metrics available
        COALESCE(ks.keyword_impressions, ags.ad_group_impressions, cb.impressions) as impressions,
        COALESCE(ks.keyword_clicks, ags.ad_group_clicks, cb.clicks) as clicks,
        COALESCE(ks.keyword_conversions, ags.ad_group_conversions, cb.conversions) as conversions,
        COALESCE(ks.keyword_conversions_value, ags.ad_group_conversions_value, cb.conversions_value) as conversions_value,  
        -- Include detail level indicators
        CASE 
            WHEN ks.keyword_cost IS NOT NULL THEN 'keyword'
            WHEN ags.ad_group_cost IS NOT NULL THEN 'ad_group'
            ELSE 'campaign'
        END as metrics_level,
        -- Include counts for debugging
        COALESCE(ags.ad_group_count, 0) as ad_group_count,
        COALESCE(ks.keyword_count, 0) as keyword_count      
    FROM campaign_base cb
    LEFT JOIN campaign_info ci ON cb.account_id = ci.customer_id AND cb.campaign_id = ci.campaign_id
    LEFT JOIN ad_group_summary ags ON cb.fulldate = ags.date AND cb.account_id = ags.customer_id AND cb.campaign_id = ags.campaign_id
    LEFT JOIN keyword_summary ks ON cb.fulldate = ks.date AND cb.account_id = ks.customer_id AND cb.campaign_id = ks.campaign_id
)
SELECT 
    fulldate,
    account_id,
    campaign_id,
    campaign_name,
    latest_status,
    advertising_channel_type,
    device,
    cost,
    impressions,
    clicks,
    conversions,
    conversions_value,
    metrics_level,
    -- Optional: include debugging fields
    ad_group_count,
    keyword_count
FROM unified