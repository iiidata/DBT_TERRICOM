

  create  table "test".test_vbd."post_attachments_data_subattachments__dbt_tmp"
  as (
    
with __dbt__CTE__post_attachments_data_subattachments_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_data_hashid,
    jsonb_extract_path(subattachments, 'data') as "data",
    _airbyte_emitted_at
from "test".test_vbd."post_attachments_data" as table_alias
where subattachments is not null
-- subattachments at post/attachments/data/subattachments
),  __dbt__CTE__post_attachments_data_subattachments_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_data_hashid,
    "data",
    _airbyte_emitted_at
from __dbt__CTE__post_attachments_data_subattachments_ab1
-- subattachments at post/attachments/data/subattachments
),  __dbt__CTE__post_attachments_data_subattachments_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(
    
    coalesce(cast(_airbyte_data_hashid as 
    varchar
), '') || '-' || coalesce(cast("data" as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_subattachments_hashid,
    tmp.*
from __dbt__CTE__post_attachments_data_subattachments_ab2 tmp
-- subattachments at post/attachments/data/subattachments
)-- Final base SQL model
select
    _airbyte_data_hashid,
    "data",
    _airbyte_emitted_at,
    _airbyte_subattachments_hashid
from __dbt__CTE__post_attachments_data_subattachments_ab3
-- subattachments at post/attachments/data/subattachments from "test".test_vbd."post_attachments_data"
  );