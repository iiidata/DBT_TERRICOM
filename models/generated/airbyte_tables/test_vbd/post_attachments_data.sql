

  create  table "test".test_vbd."post_attachments_data__dbt_tmp"
  as (
    
with __dbt__CTE__post_attachments_data_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema

select
    _airbyte_attachments_hashid,
    jsonb_extract_path_text(_airbyte_nested_data, 'url') as url,
    jsonb_extract_path_text(_airbyte_nested_data, 'type') as "type",
    
        jsonb_extract_path(_airbyte_nested_data, 'media')
     as media,
    jsonb_extract_path_text(_airbyte_nested_data, 'title') as title,
    
        jsonb_extract_path(_airbyte_nested_data, 'target')
     as target,
    
        jsonb_extract_path(_airbyte_nested_data, 'subattachments')
     as subattachments,
    _airbyte_emitted_at
from "test".test_vbd."post_attachments" as table_alias
cross join jsonb_array_elements(
        case jsonb_typeof("data")
        when 'array' then "data"
        else '[]' end
    ) as _airbyte_nested_data
where "data" is not null
-- data at post/attachments/data
),  __dbt__CTE__post_attachments_data_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_attachments_hashid,
    cast(url as 
    varchar
) as url,
    cast("type" as 
    varchar
) as "type",
    cast(media as 
    jsonb
) as media,
    cast(title as 
    varchar
) as title,
    cast(target as 
    jsonb
) as target,
    cast(subattachments as 
    jsonb
) as subattachments,
    _airbyte_emitted_at
from __dbt__CTE__post_attachments_data_ab1
-- data at post/attachments/data
),  __dbt__CTE__post_attachments_data_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(
    
    coalesce(cast(_airbyte_attachments_hashid as 
    varchar
), '') || '-' || coalesce(cast(url as 
    varchar
), '') || '-' || coalesce(cast("type" as 
    varchar
), '') || '-' || coalesce(cast(media as 
    varchar
), '') || '-' || coalesce(cast(title as 
    varchar
), '') || '-' || coalesce(cast(target as 
    varchar
), '') || '-' || coalesce(cast(subattachments as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_data_hashid,
    tmp.*
from __dbt__CTE__post_attachments_data_ab2 tmp
-- data at post/attachments/data
)-- Final base SQL model
select
    _airbyte_attachments_hashid,
    url,
    "type",
    media,
    title,
    target,
    subattachments,
    _airbyte_emitted_at,
    _airbyte_data_hashid
from __dbt__CTE__post_attachments_data_ab3
-- data at post/attachments/data from "test".test_vbd."post_attachments"
  );