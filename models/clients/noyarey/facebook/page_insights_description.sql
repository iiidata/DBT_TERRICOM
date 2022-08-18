{{ config(schema='noyarey') }}

select distinct _airbyte_data::json->>'title' as title,
    _airbyte_data::json->>'description' as description
from _airbyte_raw_page_insights