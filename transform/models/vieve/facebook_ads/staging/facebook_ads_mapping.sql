{{ 
    config(
        materialized='table',
        schema = 'matatika_facebook_ads_staging'  
    ) 
}}

With campaign_history as (

    SELECT
        id,
        account_id, 
        name,
        objective,
        status, 
        updated_time,
    FROM {{source('facebook_ads','campaigns')}}
    GROUP BY ALL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_time DESC) = 1

)

, ad_set_history as (
    SELECT
        id,
        account_id,
        name,
        status, 
        campaign_id, 
        updated_time,
    FROM {{source('facebook_ads','adsets')}} 
    GROUP BY ALL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_time DESC) = 1
)

, ad_history as (
    SELECT
        id,
        name,
        status, 
        campaign_id, 
        adset_id as ad_set_id,
        updated_time,
    FROM {{source('facebook_ads','ads')}} 
    GROUP BY ALL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_time DESC) = 1
)

SELECT
    ad.id as ad_id,
    ad.name as ad_name,
    ad.status as ad_status,
    ad.campaign_id as campaign_id,
    ad.ad_set_id as ad_set_id,
    ad_set.name as ad_set_name,
    ad_set.status as ad_set_status,
    campaign.name as campaign_name,
    campaign.objective as campaign_objective,
    campaign.status as campaign_status
    FROM ad_history ad
    LEFT JOIN ad_set_history ad_set
        ON ad.ad_set_id = ad_set.id
    LEFT JOIN campaign_history campaign
        ON ad.campaign_id = campaign.id