

  create  table "test".test_vbd."page_instagram_business_account__dbt_tmp"
  as (
    
with __dbt__CTE__page_instagram_business_account_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_page_hashid,
    jsonb_extract_path_text(instagram_business_account, 'id') as "id",
    _airbyte_emitted_at
from "test".test_vbd."page" as table_alias
where instagram_business_account is not null
-- instagram_business_account at page/instagram_business_account
),  __dbt__CTE__page_instagram_business_account_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_page_hashid,
    cast("id" as 
    varchar
) as "id",
    _airbyte_emitted_at
from __dbt__CTE__page_instagram_business_account_ab1
-- instagram_business_account at page/instagram_business_account
),  __dbt__CTE__page_instagram_business_account_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(
    
    coalesce(cast(_airbyte_page_hashid as 
    varchar
), '') || '-' || coalesce(cast("id" as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_instagram_business_account_hashid,
    tmp.*
from __dbt__CTE__page_instagram_business_account_ab2 tmp
-- instagram_business_account at page/instagram_business_account
)-- Final base SQL model
select
    _airbyte_page_hashid,
    "id",
    _airbyte_emitted_at,
    _airbyte_instagram_business_account_hashid
from __dbt__CTE__page_instagram_business_account_ab3
-- instagram_business_account at page/instagram_business_account from "test".test_vbd."page"
  );