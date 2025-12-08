SELECT
    c.channel_title,
    f.snapshot_date,
    f.view_count,
    f.subscriber_count,
    f.video_count
FROM fct_channel_daily_stats f
JOIN dim_channel c 
    ON f.channel_key = c.channel_key
ORDER BY 
    c.channel_title,
    f.snapshot_date;
