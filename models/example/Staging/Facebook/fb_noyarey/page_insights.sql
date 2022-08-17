{{ config(
    materialized="table",
    schema="fb_noyarey"
) }}

with gen_page_insights as (
    select 
       _airbyte_data::json->>'id' as page_id,
       _airbyte_data::json->>'fan_count' as fans,
       _airbyte_data::json->>'followers_count' as followers,
       _airbyte_data::json->>'were_here_count' as were_here_count,
       _airbyte_data::json->>'talking_about_count' as talking_about_count,
       _airbyte_data::json->>'new_like_count' as new_like_count,
       _airbyte_data::json->>'rating_count' as rating_count

    from fb_noyarey._airbyte_raw_page
), 

spec_page_insights as (
    select 
        split_part(_airbyte_data::json->>'id', '/', 1) as page_id,
        _airbyte_data::json->>'title' as title,
        ((_airbyte_data::json->>'values')::json->0)::json->>'value' as value,
        to_date(((_airbyte_data::json->>'values')::json->0)::json->>'end_time', 'YYYY MM DD') as record_date

    from fb_noyarey._airbyte_raw_page_insights
)

select * from spec_page_insights spi
LEFT JOIN gen_page_insights gpi using (page_id)
