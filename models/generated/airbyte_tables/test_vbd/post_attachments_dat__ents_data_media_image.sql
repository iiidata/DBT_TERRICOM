

  create  table "test".test_vbd."post_attachments_dat__ents_data_media_image__dbt_tmp"
  as (
    
with __dbt__CTE__post_attachments_dat__ents_data_media_image_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_media_hashid,
    jsonb_extract_path_text(image, 'src') as src,
    jsonb_extract_path_text(image, 'width') as width,
    jsonb_extract_path_text(image, 'height') as height,
    _airbyte_emitted_at
from "test".test_vbd."post_attachments_dat__ttachments_data_media" as table_alias
where image is not null
-- image at post/attachments/data/subattachments/data/media/image
),  __dbt__CTE__post_attachments_dat__ents_data_media_image_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_media_hashid,
    cast(src as 
    varchar
) as src,
    cast(width as 
    bigint
) as width,
    cast(height as 
    bigint
) as height,
    _airbyte_emitted_at
from __dbt__CTE__post_attachments_dat__ents_data_media_image_ab1
-- image at post/attachments/data/subattachments/data/media/image
),  __dbt__CTE__post_attachments_dat__ents_data_media_image_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(
    
    coalesce(cast(_airbyte_media_hashid as 
    varchar
), '') || '-' || coalesce(cast(src as 
    varchar
), '') || '-' || coalesce(cast(width as 
    varchar
), '') || '-' || coalesce(cast(height as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_image_hashid,
    tmp.*
from __dbt__CTE__post_attachments_dat__ents_data_media_image_ab2 tmp
-- image at post/attachments/data/subattachments/data/media/image
)-- Final base SQL model
select
    _airbyte_media_hashid,
    src,
    width,
    height,
    _airbyte_emitted_at,
    _airbyte_image_hashid
from __dbt__CTE__post_attachments_dat__ents_data_media_image_ab3
-- image at post/attachments/data/subattachments/data/media/image from "test".test_vbd."post_attachments_dat__ttachments_data_media"
  );