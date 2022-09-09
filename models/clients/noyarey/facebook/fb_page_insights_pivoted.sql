

{% set insight_names_page = ['page_fans',
'page_views_by_site_logged_in_unique',
'page_fans_city',
'page_fans_gender_age',
'page_impressions_by_age_gender_unique',
'page_views_total',
'page_views_by_referers_logged_in_unique',
'page_engaged_users',
'page_post_engagements',
'page_impressions_by_city_unique' ]
%}

select page_id, 
         {% for insight_name_page in insight_names_page %}
            max(case when (name='{{insight_name_page}}') then value else NULL end) as {{insight_name_page}},
         {% endfor %}
         record_date
from {{ ref('fb_page_insights_staging') }} 
WHERE period= 'day'
group by page_id, record_date
         