{{ config(materialized='view') }}

select 
       _airbyte_data::json->>'id' as id,
       (((_airbyte_data::json->>'specificContent')::json->>'com.linkedin.ugc.ShareContent')::json->>'shareCommentary')::json->>'text' as post,
       (((_airbyte_data::json->>'specificContent')::json->>'com.linkedin.ugc.ShareContent')::json->>'shareFeatures')::json->>'hashtags' as hashtags,
       ((_airbyte_data::json->>'specificContent')::json->>'com.linkedin.ugc.ShareContent')::json->>'shareMediaCategory' as media_type,
       _airbyte_data::json->>'author' as auteur,
       (_airbyte_data::json->>'created')::json->>'time' as date_creation

       
       
       



from li_gresivaudan._airbyte_raw_posts_organization