{{ config(materialized='view') }}

select 
       ((_airbyte_data::json->>'elements')::json->>3)


from public._airbyte_raw_posts_organization