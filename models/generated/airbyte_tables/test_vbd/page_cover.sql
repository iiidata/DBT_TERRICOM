

  create  table "test".test_vbd."page_cover__dbt_tmp"
  as (
    
with __dbt__CTE__page_cover_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_page_hashid,
    jsonb_extract_path_text(cover, 'id') as "id",
    jsonb_extract_path_text(cover, 'source') as "source",
    jsonb_extract_path_text(cover, 'cover_id') as cover_id,
    jsonb_extract_path_text(cover, 'offset_x') as offset_x,
    jsonb_extract_path_text(cover, 'offset_y') as offset_y,
    _airbyte_emitted_at
from "test".test_vbd."page" as table_alias
where cover is not null
-- cover at page/cover
),  __dbt__CTE__page_cover_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_page_hashid,
    cast("id" as 
    varchar
) as "id",
    cast("source" as 
    varchar
) as "source",
    cast(cover_id as 
    varchar
) as cover_id,
    cast(offset_x as 
    bigint
) as offset_x,
    cast(offset_y as 
    bigint
) as offset_y,
    _airbyte_emitted_at
from __dbt__CTE__page_cover_ab1
-- cover at page/cover
),  __dbt__CTE__page_cover_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(
    
    coalesce(cast(_airbyte_page_hashid as 
    varchar
), '') || '-' || coalesce(cast("id" as 
    varchar
), '') || '-' || coalesce(cast("source" as 
    varchar
), '') || '-' || coalesce(cast(cover_id as 
    varchar
), '') || '-' || coalesce(cast(offset_x as 
    varchar
), '') || '-' || coalesce(cast(offset_y as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_cover_hashid,
    tmp.*
from __dbt__CTE__page_cover_ab2 tmp
-- cover at page/cover
)-- Final base SQL model
select
    _airbyte_page_hashid,
    "id",
    "source",
    cover_id,
    offset_x,
    offset_y,
    _airbyte_emitted_at,
    _airbyte_cover_hashid
from __dbt__CTE__page_cover_ab3
-- cover at page/cover from "test".test_vbd."page"
  );