{{ config(schema='noyarey') }}

with spec_post_insights as (
    select split_part(_airbyte_data::json->>'id', '/', 1) as post_id,
        _airbyte_data::json->>'name' as name,
        (((_airbyte_data::json->>'values')::json->0)::json->>'value')::int as value, 
        cast(to_char(_airbyte_emitted_at, 'YYYY-MM-DD') as date) as record_date
    from _airbyte_raw_post_insights
),
gen_post_insights as (
    select 
        (_airbyte_data::json->>'id')::text as post_id,
        ((_airbyte_data::json->>'shares')::json->>'count')::integer as shares

    from _airbyte_raw_post
)

select * from spec_post_insights spi
LEFT JOIN gen_post_insights gpi using (post_id)
