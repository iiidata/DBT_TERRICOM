select 
       (_airbyte_data::json->>'id')::int as id,
       (_airbyte_emitted_at)::date as record_date,
       (_airbyte_data::json->>'$URN')::text as urn,
       (_airbyte_data::json->>'localizedName')::text as name,
       (_airbyte_data::json->>'localizedWebsite')::text as website,
       (_airbyte_data::json#>>'{foundedOn,year}')::int as year_page_founded,
       ((((_airbyte_data::json->>'locations')::json->0)::json->>'address')::json->>'city')::text as location_city,
       ((((_airbyte_data::json->>'locations')::json->0)::json->>'address')::json->>'line1')::text as location_address,
       ((((_airbyte_data::json->>'locations')::json->0)::json->>'address')::json->>'country')::text as location_country,
       btrim(_airbyte_data::json->>'industries','[""]') as industries,
       (_airbyte_data::json->>'vanityName')::text as lk_name,
       (_airbyte_data::json->>'localizedDescription')::text as description,
       (_airbyte_data::json->>'organizationType')::text as type_organization,
       btrim(_airbyte_data::json->>'groups','[""]') as groups,
       btrim(_airbyte_data::json->>'localizedSpecialties','[""]') as specialties
from li_gresivaudan._airbyte_raw_organization_lookup
