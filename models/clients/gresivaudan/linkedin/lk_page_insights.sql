
{{ config(materialized='view') }}

with followers_count as (

select 
       _airbyte_data::json->>'firstDegreeSize' as followers_count,
       cast(to_char(_airbyte_emitted_at, 'YYYY-MM-DD') as date) as record_date

from li_gresivaudan._airbyte_raw_total_follower_count
),

share_stats as (
select 
       _airbyte_data::json->>'organizationalEntity' as organization,
       (_airbyte_data::json->>'totalShareStatistics')::json->>'likeCount' as Likes,
       (_airbyte_data::json->>'totalShareStatistics')::json->>'clickCount' as Clicks,
       (_airbyte_data::json->>'totalShareStatistics')::json->>'engagement' as Engagement,
       (_airbyte_data::json->>'totalShareStatistics')::json->>'shareCount' as Shares,
       (_airbyte_data::json->>'totalShareStatistics')::json->>'commentCount' as Comments,
       (_airbyte_data::json->>'totalShareStatistics')::json->>'impressionCount' as Impressions,
       (_airbyte_data::json->>'totalShareStatistics')::json->>'shareMentionsCount' as Share_mentions,
       (_airbyte_data::json->>'totalShareStatistics')::json->>'commentMentionsCount' as Comment_mentions,
       (_airbyte_data::json->>'totalShareStatistics')::json->>'uniqueImpressionsCount' as Unique_impression,
       cast(to_char(_airbyte_emitted_at, 'YYYY-MM-DD') as date) as record_date


from li_gresivaudan._airbyte_raw_share_statistics
)


select * from share_stats
LEFT JOIN followers_count using (record_date)