
select
       distinct _airbyte_data::json->>'name' as name,
       _airbyte_data::json->>'title' as title,
       _airbyte_data::json->>'description' as description
from nitram._airbyte_raw_post_insights