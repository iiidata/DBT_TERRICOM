{{ config(schema='noyarey') }}

with gen_page_insights as (
    select 
       _airbyte_data::json->>'id' as page_id,
       _airbyte_data::json->>'fan_count' as fans,
       _airbyte_data::json->>'followers_count' as followers,
       _airbyte_data::json->>'were_here_count' as were_here_count,
       _airbyte_data::json->>'talking_about_count' as talking_about_count,
       _airbyte_data::json->>'new_like_count' as new_like_count,
       _airbyte_data::json->>'rating_count' as rating_count

    from _airbyte_raw_page
), 

spec_page_insights as (
    select 
        split_part(_airbyte_data::json->>'id', '/', 1) as page_id,
        _airbyte_data::json->>'title' as title,
        _airbyte_data::json->>'name' as name, 
        ((_airbyte_data::json->>'values')::json->0)::json->>'value' as value,
        cast(to_char(_airbyte_emitted_at, 'YYYY-MM-DD') as date) as record_date,
        _airbyte_data::json->>'period' as period

    from _airbyte_raw_page_insights
    
)

select * from spec_page_insights spi
LEFT JOIN gen_page_insights gpi using (page_id)
