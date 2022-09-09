



{% set insight_names = ["post_engaged_users", "post_negative_feedback_unique", "post_engaged_fan","post_impressions_unique",
        "post_impressions_fan_unique", "post_impressions_organic_unique", "post_impressions_viral_unique", "post_reactions_like_total",
        "post_reactions_haha_total", "post_reactions_love_total", "post_reactions_wow_total", "post_reactions_sorry_total", "post_reactions_angers_total"
    ]
%}

select post_id, 
         {% for insight_name in insight_names %}
            max(case when (name='{{insight_name}}') then value else NULL end) as {{insight_name}},
         {% endfor %}
        record_date
        from {{ ref('fb_post_insights_staging') }} 
         group by post_id, record_date
         
