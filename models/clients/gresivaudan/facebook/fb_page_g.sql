
select 
       _airbyte_data::json->>'id' as id,
       _airbyte_data::json->>'name' as nom,
       _airbyte_data::json->>'phone' as telephone,
       btrim(_airbyte_data::json->>'emails','[""]') as email,
       _airbyte_data::json->>'website' as website,
       _airbyte_data::json#>>'{location,zip}' as code_postal,
       _airbyte_data::json#>>'{location,street}' as adresse,
       _airbyte_data::json#>>'{location,country}' as pays,
       _airbyte_data::json#>'{location,latitude}' as latitude,
       _airbyte_data::json#>'{location,longitude}' as longitude,
       _airbyte_data::json->>'fan_count' as fans,
       _airbyte_data::json->>'followers_count' as followers,
       _airbyte_data::json->>'were_here_count' as were_here_count,
       _airbyte_data::json->>'talking_about_count' as talking_about_count,
       _airbyte_data::json->>'new_like_count' as new_like_count,
       _airbyte_data::json->>'rating_count' as rating_count
from fb_noyarey._airbyte_raw_page

       

