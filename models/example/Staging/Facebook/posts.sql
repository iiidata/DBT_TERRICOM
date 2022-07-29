

select id, 
       shares::json->'count' as share,
       message, 
       story as activity, 
       permalink_url as url_post,
       comments as comments,
       ((attachments::json->>'data')::json->0)::json->>'url' as url_attachement,
       ((attachments::json->>'data')::json->0)::json->>'type' as type_attachement,
       ((attachments::json->>'data')::json->0)::json->>'title' as title_attachement,
       ((((attachments::json->>'data')::json->0)::json->>'media')::json->>'image')::json->>'src' as src_image,
       split_part(created_time,'T',1) as date_creation,
       split_part(created_time, 'T', 2) as time_creation,
       _airbyte_emitted_at as pull_time
       
from public.post
