

  create  table "test".test_vbd."post_attachments__dbt_tmp"
  as (
    
with __dbt__CTE__post_attachments_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_post_hashid,
    jsonb_extract_path(attachments, 'data') as "data",
    _airbyte_emitted_at
from "test".test_vbd."post" as table_alias
where attachments is not null
-- attachments at post/attachments
),  __dbt__CTE__post_attachments_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_post_hashid,
    "data",
    _airbyte_emitted_at
from __dbt__CTE__post_attachments_ab1
-- attachments at post/attachments
),  __dbt__CTE__post_attachments_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(
    
    coalesce(cast(_airbyte_post_hashid as 
    varchar
), '') || '-' || coalesce(cast("data" as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_attachments_hashid,
    tmp.*
from __dbt__CTE__post_attachments_ab2 tmp
-- attachments at post/attachments
)-- Final base SQL model
select
    _airbyte_post_hashid,
    "data",
    _airbyte_emitted_at,
    _airbyte_attachments_hashid
from __dbt__CTE__post_attachments_ab3
-- attachments at post/attachments from "test".test_vbd."post"
  );