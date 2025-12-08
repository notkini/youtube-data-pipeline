WITH latest_views AS (
    SELECT
        v.video_id,
        v.video_title,
        c.channel_title,
        MAX(f.view_count) AS max_views
    FROM fct_video_daily_stats f
    JOIN dim_video v ON f.video_key = v.video_key
    JOIN dim_channel c ON v.channel_key = c.channel_key
    GROUP BY v.video_id, v.video_title, c.channel_title
)
SELECT *
FROM latest_views
ORDER BY max_views DESC
LIMIT 20;
