 with facebook_page_insights as (
	select _airbyte_data::json->>'name' as name,
	   _airbyte_data::json->>'title' as title,
	   _airbyte_data::json->>'description' as description, 
	   _airbyte_data::json->>'values' as value



from public."_airbyte_raw_page_insights" 
 )

select name, 
	   title, 	  
	   description, 
	   (value::json->0)::json->>'value' as value,
	   to_date((value::json->0)::json->>'end_time', 'YYYY MM DD') as record_date
   
   
from facebook_page_insights fp

