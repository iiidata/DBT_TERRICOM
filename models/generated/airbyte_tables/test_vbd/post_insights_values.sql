

  create  table "test".test_vbd."post_insights_values__dbt_tmp"
  as (
    
with __dbt__CTE__post_insights_values_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema

select
    _airbyte_post_insights_hashid,
    
        jsonb_extract_path(_airbyte_nested_data, 'value')
     as "value",
    jsonb_extract_path_text(_airbyte_nested_data, 'end_time') as end_time,
    _airbyte_emitted_at
from "test".test_vbd."post_insights" as table_alias
cross join jsonb_array_elements(
        case jsonb_typeof("values")
        when 'array' then "values"
        else '[]' end
    ) as _airbyte_nested_data
where "values" is not null
-- values at post_insights/values
),  __dbt__CTE__post_insights_values_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_post_insights_hashid,
    cast("value" as 
    jsonb
) as "value",
    cast(nullif(end_time, '') as 
    timestamp with time zone
) as end_time,
    _airbyte_emitted_at
from __dbt__CTE__post_insights_values_ab1
-- values at post_insights/values
),  __dbt__CTE__post_insights_values_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(
    
    coalesce(cast(_airbyte_post_insights_hashid as 
    varchar
), '') || '-' || coalesce(cast("value" as 
    varchar
), '') || '-' || coalesce(cast(end_time as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_values_hashid,
    tmp.*
from __dbt__CTE__post_insights_values_ab2 tmp
-- values at post_insights/values
)-- Final base SQL model
select
    _airbyte_post_insights_hashid,
    "value",
    end_time,
    _airbyte_emitted_at,
    _airbyte_values_hashid
from __dbt__CTE__post_insights_values_ab3
-- values at post_insights/values from "test".test_vbd."post_insights"
  );