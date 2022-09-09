
    select 
        split_part(_airbyte_data::json->>'id', '/', 1) as page_id,
        _airbyte_data::json->>'title' as title,
        _airbyte_data::json->>'name' as name, 
        ((_airbyte_data::json->>'values')::json->0)::json->>'value' as value,
        cast(to_char(_airbyte_emitted_at, 'YYYY-MM-DD') as date) as record_date,
        _airbyte_data::json->>'period' as period

    from fb_gresivaudan._airbyte_raw_page_insights

