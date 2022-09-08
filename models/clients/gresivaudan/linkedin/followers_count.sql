{{ config(materialized='view') }}

select 
       _airbyte_data::json->>'firstDegreeSize' as followers_count,
       cast(to_char(_airbyte_emitted_at, 'YYYY-MM-DD') as date) as record_date

from li_gresivaudan._airbyte_raw_total_follower_count