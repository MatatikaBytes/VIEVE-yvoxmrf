
{{ 
    config(
        materialized='table',
        schema = 'matatika_google_ads'  
    ) 
}}
with campaigns as (
    SELECT
        campaign__id
        , campaign__name
        , campaign__status
        , campaign__advertising_channel_type
        , _sdc_extracted_at
    FROM {{source('google_ads','campaign_history')}} history
    union all
    SELECT
        campaign__id
        , campaign__name
        , campaign__status
        , campaign__advertising_channel_type
        , _sdc_extracted_at
    FROM {{source('google_ads','campaign_history_unsegmented')}} history
)
select * from campaigns