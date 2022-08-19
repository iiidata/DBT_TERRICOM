

  create  table "test".test_vbd."post_shares__dbt_tmp"
  as (
    
with __dbt__CTE__post_shares_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_post_hashid,
    jsonb_extract_path_text(shares, 'count') as "count",
    _airbyte_emitted_at
from "test".test_vbd."post" as table_alias
where shares is not null
-- shares at post/shares
),  __dbt__CTE__post_shares_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_post_hashid,
    cast("count" as 
    bigint
) as "count",
    _airbyte_emitted_at
from __dbt__CTE__post_shares_ab1
-- shares at post/shares
),  __dbt__CTE__post_shares_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(
    
    coalesce(cast(_airbyte_post_hashid as 
    varchar
), '') || '-' || coalesce(cast("count" as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_shares_hashid,
    tmp.*
from __dbt__CTE__post_shares_ab2 tmp
-- shares at post/shares
)-- Final base SQL model
select
    _airbyte_post_hashid,
    "count",
    _airbyte_emitted_at,
    _airbyte_shares_hashid
from __dbt__CTE__post_shares_ab3
-- shares at post/shares from "test".test_vbd."post"
  );