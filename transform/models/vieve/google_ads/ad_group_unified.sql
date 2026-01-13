{{ 
    config(
        materialized='table',
        schema = 'matatika_google_ads'  
    ) 
}}
with ad_group as (
    SELECT 
        ad_group__id
        , ad_group__name
        , _sdc_extracted_at
    FROM {{source('google_ads','adgroups')}} 
    union all
    SELECT
        ad_group__id
        , ad_group__name
        , _sdc_extracted_at
    FROM {{source('google_ads','ad_group_unsegmented')}}
)
select * from ad_group