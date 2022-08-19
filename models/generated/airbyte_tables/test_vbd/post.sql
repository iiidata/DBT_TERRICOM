

  create  table "test".test_vbd."post__dbt_tmp"
  as (
    
with __dbt__CTE__post_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    jsonb_extract_path_text(_airbyte_data, 'id') as "id",
    jsonb_extract_path_text(_airbyte_data, 'story') as story,
    
        jsonb_extract_path(table_alias._airbyte_data, 'shares')
     as shares,
    jsonb_extract_path_text(_airbyte_data, 'message') as message,
    
        jsonb_extract_path(table_alias._airbyte_data, 'attachments')
     as attachments,
    jsonb_extract_path_text(_airbyte_data, 'created_time') as created_time,
    jsonb_extract_path_text(_airbyte_data, 'permalink_url') as permalink_url,
    _airbyte_emitted_at
from "test".test_vbd._airbyte_raw_post as table_alias
-- post
),  __dbt__CTE__post_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    cast("id" as 
    varchar
) as "id",
    cast(story as 
    varchar
) as story,
    cast(shares as 
    jsonb
) as shares,
    cast(message as 
    varchar
) as message,
    cast(attachments as 
    jsonb
) as attachments,
    cast(created_time as 
    varchar
) as created_time,
    cast(permalink_url as 
    varchar
) as permalink_url,
    _airbyte_emitted_at
from __dbt__CTE__post_ab1
-- post
),  __dbt__CTE__post_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(
    
    coalesce(cast("id" as 
    varchar
), '') || '-' || coalesce(cast(story as 
    varchar
), '') || '-' || coalesce(cast(shares as 
    varchar
), '') || '-' || coalesce(cast(message as 
    varchar
), '') || '-' || coalesce(cast(attachments as 
    varchar
), '') || '-' || coalesce(cast(created_time as 
    varchar
), '') || '-' || coalesce(cast(permalink_url as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_post_hashid,
    tmp.*
from __dbt__CTE__post_ab2 tmp
-- post
)-- Final base SQL model
select
    "id",
    story,
    shares,
    message,
    attachments,
    created_time,
    permalink_url,
    _airbyte_emitted_at,
    _airbyte_post_hashid
from __dbt__CTE__post_ab3
-- post from "test".test_vbd._airbyte_raw_post
  );