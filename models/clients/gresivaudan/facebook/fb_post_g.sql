
select (_airbyte_data::json->>'id')::text as id,
       ((_airbyte_data::json->>'shares')::json->>'count')::integer as share,
       (_airbyte_data::json->> 'message')::text as message, 
       (_airbyte_data::json->>'story')::text as activity, 
       (_airbyte_data::json->>'permalink_url')::text as url_post,
       --comments as comments,
       ((((_airbyte_data::json->>'attachments')::json->>'data')::json->0)::json->>'url')::text as url_attachement,
       ((((_airbyte_data::json->>'attachments')::json->>'data')::json->0)::json->>'type')::text  as type_attachement,
       ((((_airbyte_data::json->>'attachments')::json->>'data')::json->0)::json->>'title')::text  as title_attachement,
       ((((((_airbyte_data::json->>'attachments')::json->>'data')::json->0)::json->>'media')::json->>'image')::json->>'src')::text as src_image,
       cast(split_part((_airbyte_data::json->>'created_time'),'T',1) as date) as date_creation,
       cast(split_part((_airbyte_data::json->>'created_time'), 'T', 2) as time) as time_creation,
       cast(to_char(_airbyte_emitted_at, 'YYYY-MM-DD') as date) as record_date
       
from fb_gresivaudan._airbyte_raw_post
