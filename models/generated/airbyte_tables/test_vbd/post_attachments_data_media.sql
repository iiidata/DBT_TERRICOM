

  create  table "test".test_vbd."post_attachments_data_media__dbt_tmp"
  as (
    
with __dbt__CTE__post_attachments_data_media_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_data_hashid,
    
        jsonb_extract_path(table_alias.media, 'image')
     as image,
    _airbyte_emitted_at
from "test".test_vbd."post_attachments_data" as table_alias
where media is not null
-- media at post/attachments/data/media
),  __dbt__CTE__post_attachments_data_media_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_data_hashid,
    cast(image as 
    jsonb
) as image,
    _airbyte_emitted_at
from __dbt__CTE__post_attachments_data_media_ab1
-- media at post/attachments/data/media
),  __dbt__CTE__post_attachments_data_media_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(
    
    coalesce(cast(_airbyte_data_hashid as 
    varchar
), '') || '-' || coalesce(cast(image as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_media_hashid,
    tmp.*
from __dbt__CTE__post_attachments_data_media_ab2 tmp
-- media at post/attachments/data/media
)-- Final base SQL model
select
    _airbyte_data_hashid,
    image,
    _airbyte_emitted_at,
    _airbyte_media_hashid
from __dbt__CTE__post_attachments_data_media_ab3
-- media at post/attachments/data/media from "test".test_vbd."post_attachments_data"
  );