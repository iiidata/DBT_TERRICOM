
    select split_part(_airbyte_data::json->>'id', '/', 1) as post_id,
        _airbyte_data::json->>'name' as name,
        (((_airbyte_data::json->>'values')::json->0)::json->>'value')::int as value, 
        cast(to_char(_airbyte_emitted_at, 'YYYY-MM-DD') as date) as record_date
    from fb_gresivaudan._airbyte_raw_post_insights
