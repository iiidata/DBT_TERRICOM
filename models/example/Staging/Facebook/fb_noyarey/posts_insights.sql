{{ config(
    materialized="table",
    schema="fb_noyarey"
) }}


--with blabla as (
select split_part(_airbyte_data::json->>'id', '/', 1) as post_id,
       _airbyte_data::json->>'name' as name,
       (((_airbyte_data::json->>'values')::json->0)::json->>'value')::int as value, 
       cast(to_char(_airbyte_emitted_at, 'YYYY-MM-DD') as date) as date_pull
       --cast(split_part(_airbyte_emitted_at,' ',1) as date) as date_pull
from fb_noyarey._airbyte_raw_post_insights
--)