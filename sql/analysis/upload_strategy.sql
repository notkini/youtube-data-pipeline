-- Upload strategy: performance by day of week for each channel

WITH video_with_views AS (
    SELECT
        v.video_id,
        v.video_title,
        v.published_at,
        c.channel_title,
        MAX(f.view_count) AS max_views,
        MAX(f.like_count) AS max_likes
    FROM dim_video v
    JOIN dim_channel c 
        ON v.channel_key = c.channel_key
    JOIN fct_video_daily_stats f 
        ON v.video_key = f.video_key
    GROUP BY 
        v.video_id,
        v.video_title,
        v.published_at,
        c.channel_title
),
with_dow AS (
    SELECT
        channel_title,
        strftime(published_at, '%w') AS day_of_week,
        max_views,
        max_likes
    FROM video_with_views
    WHERE published_at IS NOT NULL
)
SELECT
    channel_title,
    day_of_week,                 -- 0 = Sunday, 6 = Saturday
    AVG(max_views) AS avg_views,
    AVG(max_likes) AS avg_likes,
    COUNT(*) AS video_count
FROM with_dow
GROUP BY 
    channel_title,
    day_of_week
ORDER BY 
    channel_title,
    avg_views DESC;
