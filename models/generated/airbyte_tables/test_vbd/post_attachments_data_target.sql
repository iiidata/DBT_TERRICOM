

  create  table "test".test_vbd."post_attachments_data_target__dbt_tmp"
  as (
    
with __dbt__CTE__post_attachments_data_target_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_data_hashid,
    jsonb_extract_path_text(target, 'id') as "id",
    jsonb_extract_path_text(target, 'url') as url,
    _airbyte_emitted_at
from "test".test_vbd."post_attachments_data" as table_alias
where target is not null
-- target at post/attachments/data/target
),  __dbt__CTE__post_attachments_data_target_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_data_hashid,
    cast("id" as 
    varchar
) as "id",
    cast(url as 
    varchar
) as url,
    _airbyte_emitted_at
from __dbt__CTE__post_attachments_data_target_ab1
-- target at post/attachments/data/target
),  __dbt__CTE__post_attachments_data_target_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(
    
    coalesce(cast(_airbyte_data_hashid as 
    varchar
), '') || '-' || coalesce(cast("id" as 
    varchar
), '') || '-' || coalesce(cast(url as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_target_hashid,
    tmp.*
from __dbt__CTE__post_attachments_data_target_ab2 tmp
-- target at post/attachments/data/target
)-- Final base SQL model
select
    _airbyte_data_hashid,
    "id",
    url,
    _airbyte_emitted_at,
    _airbyte_target_hashid
from __dbt__CTE__post_attachments_data_target_ab3
-- target at post/attachments/data/target from "test".test_vbd."post_attachments_data"
  );