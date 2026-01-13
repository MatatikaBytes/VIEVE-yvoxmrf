/*
CONTEXT:
- Creating the Google ads table, so we can get metrics such as impressions, clicks, conversions and conversion together
RESULT EXPECTATION:
- Should be unique by date campaign name/ campaign id
ASSUMPTION:
-
*/
{{ 
    config(
        materialized='table',
        schema = 'staging'  
    ) 
}}
-- Getting the campaign name from the history name
WITH campaign_name_table AS (
  
    SELECT
        campaign__id as id
        , campaign__name as name
        , campaign__advertising_channel_type as ADVERTISING_CHANNEL_TYPE
        , campaign__status as status
    FROM  {{source('google_ads','campaign_history')}} history 
-- camapaign id should be unique but adding qualify statement to make sure
qualify row_number() over (partition by campaign__id order by _sdc_extracted_at desc) = 1
)
, keyword_name_table AS (
SELECT
    ad_group_criterion__criterion_id as id
    , ad_group_criterion__keyword__text as keyword_text
    , ad_group_criterion__age_range__type as AGE_RANGE_TYPE
    , ad_group_criterion__gender__type as GENDER_TYPE
    , ad_group_criterion__income_range__type as INCOME_RANGE_TYPE
    , ad_group_criterion__keyword__match_type as KEYWORD_MATCH_TYPE
FROM  {{source('google_ads','ad_group_criterion')}} history 
qualify row_number() over (partition by ad_group_criterion__criterion_id order by _sdc_extracted_at desc) = 1
)
, adgroup_name_table AS (
    SELECT 
        ad_group__id as id
        , ad_group__name AS adgroup_name
    FROM  {{source('google_ads','adgroups')}} history 
    -- adgroup id should be unique but adding qualify statement to make sure
    qualify row_number() over (partition by ad_group__id order by _sdc_extracted_at desc) = 1 
) 
, account_history AS (
    SELECT
        *
    FROM {{source('google_ads','customer')}}
-- account id should be unique but adding qualify statement to make sure
qualify row_number() over (partition by customer__id order by _sdc_extracted_at desc) = 1

)
, keyword_stats AS (
SELECT 
    stats.segments__date as fulldate
    , stats.ad_group_criterion__criterion_id as AD_GROUP_CRITERION_CRITERION_ID
    , keyword.keyword_text as keyword_text
    , keyword.KEYWORD_MATCH_TYPE
    , keyword.AGE_RANGE_TYPE
    , keyword.GENDER_TYPE
    , keyword.INCOME_RANGE_TYPE
    , history.id as campaign_id
    , history.name as campaign_name
    , history.status as latest_status
    , stats.ad_group__id AS adgroup_id
    , adgroup.adgroup_name 
    , stats.segments__device as device
    , account.customer__id as account_id
    , account.customer__currency_code as currency_code
    , account.customer__descriptive_name as DESCRIPTIVE_NAME
    , history.ADVERTISING_CHANNEL_TYPE
    , sum(metrics__impressions) as impressions
    , sum(metrics__clicks) as clicks
    , sum(metrics__cost_micros/1000000) as cost
    , sum(metrics__conversions) as conversions
    , sum(metrics__conversions_value) as conversions_value
-- keyword_stats is where you get the keyword data from 
FROM {{source('google_ads','keyword_stats')}} as stats
LEFT JOIN campaign_name_table as history ON stats.campaign__id = history.id
LEFT JOIN keyword_name_table as keyword ON stats.ad_group_criterion__criterion_id = keyword.id
LEFT JOIN adgroup_name_table as adgroup ON stats.ad_group__id = adgroup.id
LEFT JOIN account_history as account ON stats.customer__id = account.customer__id
WHERE stats.segments__date >= '2022-01-01'
GROUP BY ALL
)
SELECT 
  fulldate
  , account_id
  , DESCRIPTIVE_NAME
  , AD_GROUP_CRITERION_CRITERION_ID
  , INITCAP(keyword_text) AS keyword_text
  , KEYWORD_MATCH_TYPE
  , AGE_RANGE_TYPE
  , GENDER_TYPE
  , INCOME_RANGE_TYPE
  , campaign_id
  , campaign_name
  , CASE 
        WHEN UPPER(latest_status) = 'ENABLED' THEN 'ACTIVE' 
        WHEN UPPER(latest_status) = 'PAUSED' THEN 'PAUSED' 
      ELSE latest_status END AS latest_status
  , adgroup_id
  , INITCAP(adgroup_name) AS adgroup_name
  , device
  , ADVERTISING_CHANNEL_TYPE
  , sum(impressions) as impressions
  , sum(clicks) as clicks
  , sum(cost) AS cost
  , sum(conversions) as conversions
  , sum(conversions_value) AS conversions_value
FROM keyword_stats
GROUP BY ALL