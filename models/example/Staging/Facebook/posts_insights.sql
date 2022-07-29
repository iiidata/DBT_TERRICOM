

select _airbyte_data::json->>'id' as post_id,
       _airbyte_data::json->>'name' as name,
       _airbyte_data::json->>'title' as title,
       _airbyte_data::json->>'description' as description, 
       ((_airbyte_data::json->>'values')::json->0)::json->'value' as value
from public._airbyte_raw_post_insights
