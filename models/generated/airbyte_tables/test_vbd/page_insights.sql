

  create  table "test".test_vbd."page_insights__dbt_tmp"
  as (
    
with __dbt__CTE__page_insights_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    jsonb_extract_path_text(_airbyte_data, 'id') as "id",
    jsonb_extract_path_text(_airbyte_data, 'name') as "name",
    jsonb_extract_path_text(_airbyte_data, 'title') as title,
    jsonb_extract_path_text(_airbyte_data, 'period') as "period",
    jsonb_extract_path(_airbyte_data, 'values') as "values",
    jsonb_extract_path_text(_airbyte_data, 'description') as description,
    _airbyte_emitted_at
from "test".test_vbd._airbyte_raw_page_insights as table_alias
-- page_insights
),  __dbt__CTE__page_insights_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    cast("id" as 
    varchar
) as "id",
    cast("name" as 
    varchar
) as "name",
    cast(title as 
    varchar
) as title,
    cast("period" as 
    varchar
) as "period",
    "values",
    cast(description as 
    varchar
) as description,
    _airbyte_emitted_at
from __dbt__CTE__page_insights_ab1
-- page_insights
),  __dbt__CTE__page_insights_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(
    
    coalesce(cast("id" as 
    varchar
), '') || '-' || coalesce(cast("name" as 
    varchar
), '') || '-' || coalesce(cast(title as 
    varchar
), '') || '-' || coalesce(cast("period" as 
    varchar
), '') || '-' || coalesce(cast("values" as 
    varchar
), '') || '-' || coalesce(cast(description as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_page_insights_hashid,
    tmp.*
from __dbt__CTE__page_insights_ab2 tmp
-- page_insights
)-- Final base SQL model
select
    "id",
    "name",
    title,
    "period",
    "values",
    description,
    _airbyte_emitted_at,
    _airbyte_page_insights_hashid
from __dbt__CTE__page_insights_ab3
-- page_insights from "test".test_vbd._airbyte_raw_page_insights
  );