{{ config(
    materialized="table",
    schema="fb_noyarey"
) }}

select _airbyte_data::json->>'title' as title,
	   ((_airbyte_data::json->>'values')::json->0)::json->>'value' as value,
	   to_date(((_airbyte_data::json->>'values')::json->0)::json->>'end_time', 'YYYY MM DD') as record_date



from fb_noyarey."_airbyte_raw_page_insights" 

