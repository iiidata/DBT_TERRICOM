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
       _airbyte_data::json#>'{location,longitude}' as longitude
from test_vbd._airbyte_raw_page