

  create  table "test".test_vbd."page_location__dbt_tmp"
  as (
    
with __dbt__CTE__page_location_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    _airbyte_page_hashid,
    jsonb_extract_path_text("location", 'zip') as zip,
    jsonb_extract_path_text("location", 'city') as city,
    jsonb_extract_path_text("location", 'street') as street,
    jsonb_extract_path_text("location", 'country') as country,
    jsonb_extract_path_text("location", 'latitude') as latitude,
    jsonb_extract_path_text("location", 'longitude') as longitude,
    _airbyte_emitted_at
from "test".test_vbd."page" as table_alias
where "location" is not null
-- location at page/location
),  __dbt__CTE__page_location_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _airbyte_page_hashid,
    cast(zip as 
    varchar
) as zip,
    cast(city as 
    varchar
) as city,
    cast(street as 
    varchar
) as street,
    cast(country as 
    varchar
) as country,
    cast(latitude as 
    float
) as latitude,
    cast(longitude as 
    float
) as longitude,
    _airbyte_emitted_at
from __dbt__CTE__page_location_ab1
-- location at page/location
),  __dbt__CTE__page_location_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(
    
    coalesce(cast(_airbyte_page_hashid as 
    varchar
), '') || '-' || coalesce(cast(zip as 
    varchar
), '') || '-' || coalesce(cast(city as 
    varchar
), '') || '-' || coalesce(cast(street as 
    varchar
), '') || '-' || coalesce(cast(country as 
    varchar
), '') || '-' || coalesce(cast(latitude as 
    varchar
), '') || '-' || coalesce(cast(longitude as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_location_hashid,
    tmp.*
from __dbt__CTE__page_location_ab2 tmp
-- location at page/location
)-- Final base SQL model
select
    _airbyte_page_hashid,
    zip,
    city,
    street,
    country,
    latitude,
    longitude,
    _airbyte_emitted_at,
    _airbyte_location_hashid
from __dbt__CTE__page_location_ab3
-- location at page/location from "test".test_vbd."page"
  );