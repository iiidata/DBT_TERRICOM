
with facebook_post_insights as (
SELECT 
    SUBSTRING(id, 1, 31) AS post_id, 
    name, 
    title, 
    values, 
    description
FROM 
    fb_noyarey.post_insights
),

facebook_post as (
SELECT 
    id as id, 
    message
FROM 
    fb_noyarey.post
)


select * 
from facebook_post_insights
        LEFT JOIN facebook_post
            ON facebook_post_insights.post_id = facebook_post.id
GROUP BY
    post_id,
    id,
    title,
    description,
    name,
    values,
    message
ORDER BY 
    post_id
    

