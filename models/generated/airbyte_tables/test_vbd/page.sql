

  create  table "test".test_vbd."page__dbt_tmp"
  as (
    
with __dbt__CTE__page_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    jsonb_extract_path_text(_airbyte_data, 'id') as "id",
    jsonb_extract_path_text(_airbyte_data, 'link') as "link",
    jsonb_extract_path_text(_airbyte_data, 'name') as "name",
    jsonb_extract_path_text(_airbyte_data, 'about') as about,
    
        jsonb_extract_path(table_alias._airbyte_data, 'cover')
     as cover,
    jsonb_extract_path_text(_airbyte_data, 'phone') as phone,
    jsonb_extract_path(_airbyte_data, 'emails') as emails,
    jsonb_extract_path_text(_airbyte_data, 'website') as website,
    jsonb_extract_path_text(_airbyte_data, 'can_post') as can_post,
    jsonb_extract_path_text(_airbyte_data, 'category') as category,
    
        jsonb_extract_path(table_alias._airbyte_data, 'location')
     as "location",
    jsonb_extract_path_text(_airbyte_data, 'username') as username,
    jsonb_extract_path_text(_airbyte_data, 'fan_count') as fan_count,
    jsonb_extract_path_text(_airbyte_data, 'can_checkin') as can_checkin,
    jsonb_extract_path_text(_airbyte_data, 'description') as description,
    jsonb_extract_path_text(_airbyte_data, 'rating_count') as rating_count,
    jsonb_extract_path_text(_airbyte_data, 'new_like_count') as new_like_count,
    jsonb_extract_path_text(_airbyte_data, 'display_subtext') as display_subtext,
    jsonb_extract_path_text(_airbyte_data, 'followers_count') as followers_count,
    jsonb_extract_path_text(_airbyte_data, 'were_here_count') as were_here_count,
    jsonb_extract_path_text(_airbyte_data, 'country_page_likes') as country_page_likes,
    jsonb_extract_path_text(_airbyte_data, 'single_line_address') as single_line_address,
    jsonb_extract_path_text(_airbyte_data, 'talking_about_count') as talking_about_count,
    jsonb_extract_path_text(_airbyte_data, 'global_brand_page_name') as global_brand_page_name,
    
        jsonb_extract_path(table_alias._airbyte_data, 'instagram_business_account')
     as instagram_business_account,
    jsonb_extract_path_text(_airbyte_data, 'name_with_location_descriptor') as name_with_location_descriptor,
    jsonb_extract_path_text(_airbyte_data, 'displayed_message_response_time') as displayed_message_response_time,
    _airbyte_emitted_at
from "test".test_vbd._airbyte_raw_page as table_alias
-- page
),  __dbt__CTE__page_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    cast("id" as 
    varchar
) as "id",
    cast("link" as 
    varchar
) as "link",
    cast("name" as 
    varchar
) as "name",
    cast(about as 
    varchar
) as about,
    cast(cover as 
    jsonb
) as cover,
    cast(phone as 
    varchar
) as phone,
    emails,
    cast(website as 
    varchar
) as website,
    cast(can_post as boolean) as can_post,
    cast(category as 
    varchar
) as category,
    cast("location" as 
    jsonb
) as "location",
    cast(username as 
    varchar
) as username,
    cast(fan_count as 
    bigint
) as fan_count,
    cast(can_checkin as boolean) as can_checkin,
    cast(description as 
    varchar
) as description,
    cast(rating_count as 
    bigint
) as rating_count,
    cast(new_like_count as 
    bigint
) as new_like_count,
    cast(display_subtext as 
    varchar
) as display_subtext,
    cast(followers_count as 
    bigint
) as followers_count,
    cast(were_here_count as 
    bigint
) as were_here_count,
    cast(country_page_likes as 
    bigint
) as country_page_likes,
    cast(single_line_address as 
    varchar
) as single_line_address,
    cast(talking_about_count as 
    bigint
) as talking_about_count,
    cast(global_brand_page_name as 
    varchar
) as global_brand_page_name,
    cast(instagram_business_account as 
    jsonb
) as instagram_business_account,
    cast(name_with_location_descriptor as 
    varchar
) as name_with_location_descriptor,
    cast(displayed_message_response_time as 
    varchar
) as displayed_message_response_time,
    _airbyte_emitted_at
from __dbt__CTE__page_ab1
-- page
),  __dbt__CTE__page_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(
    
    coalesce(cast("id" as 
    varchar
), '') || '-' || coalesce(cast("link" as 
    varchar
), '') || '-' || coalesce(cast("name" as 
    varchar
), '') || '-' || coalesce(cast(about as 
    varchar
), '') || '-' || coalesce(cast(cover as 
    varchar
), '') || '-' || coalesce(cast(phone as 
    varchar
), '') || '-' || coalesce(cast(emails as 
    varchar
), '') || '-' || coalesce(cast(website as 
    varchar
), '') || '-' || coalesce(cast(can_post as 
    varchar
), '') || '-' || coalesce(cast(category as 
    varchar
), '') || '-' || coalesce(cast("location" as 
    varchar
), '') || '-' || coalesce(cast(username as 
    varchar
), '') || '-' || coalesce(cast(fan_count as 
    varchar
), '') || '-' || coalesce(cast(can_checkin as 
    varchar
), '') || '-' || coalesce(cast(description as 
    varchar
), '') || '-' || coalesce(cast(rating_count as 
    varchar
), '') || '-' || coalesce(cast(new_like_count as 
    varchar
), '') || '-' || coalesce(cast(display_subtext as 
    varchar
), '') || '-' || coalesce(cast(followers_count as 
    varchar
), '') || '-' || coalesce(cast(were_here_count as 
    varchar
), '') || '-' || coalesce(cast(country_page_likes as 
    varchar
), '') || '-' || coalesce(cast(single_line_address as 
    varchar
), '') || '-' || coalesce(cast(talking_about_count as 
    varchar
), '') || '-' || coalesce(cast(global_brand_page_name as 
    varchar
), '') || '-' || coalesce(cast(instagram_business_account as 
    varchar
), '') || '-' || coalesce(cast(name_with_location_descriptor as 
    varchar
), '') || '-' || coalesce(cast(displayed_message_response_time as 
    varchar
), '')

 as 
    varchar
)) as _airbyte_page_hashid,
    tmp.*
from __dbt__CTE__page_ab2 tmp
-- page
)-- Final base SQL model
select
    "id",
    "link",
    "name",
    about,
    cover,
    phone,
    emails,
    website,
    can_post,
    category,
    "location",
    username,
    fan_count,
    can_checkin,
    description,
    rating_count,
    new_like_count,
    display_subtext,
    followers_count,
    were_here_count,
    country_page_likes,
    single_line_address,
    talking_about_count,
    global_brand_page_name,
    instagram_business_account,
    name_with_location_descriptor,
    displayed_message_response_time,
    _airbyte_emitted_at,
    _airbyte_page_hashid
from __dbt__CTE__page_ab3
-- page from "test".test_vbd._airbyte_raw_page
  );