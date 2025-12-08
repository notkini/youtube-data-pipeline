from pathlib import Path

import pandas as pd


def _ensure_warehouse_dir() -> Path:
    warehouse_dir = Path("data") / "warehouse"
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    return warehouse_dir


def build_warehouse() -> None:
    """
    Build dimension and fact tables from staging data and save to warehouse.

    Reads:
        data/staging/channels/channels.parquet
        data/staging/videos/videos.parquet

    Writes:
        data/warehouse/dim_channel.parquet
        data/warehouse/dim_video.parquet
        data/warehouse/fct_channel_daily_stats.parquet
        data/warehouse/fct_video_daily_stats.parquet
    """
    staging_channels_path = Path("data") / "staging" / "channels" / "channels.parquet"
    staging_videos_path = Path("data") / "staging" / "videos" / "videos.parquet"

    if not staging_channels_path.exists():
        raise FileNotFoundError(f"Staging channels file not found: {staging_channels_path}")
    if not staging_videos_path.exists():
        raise FileNotFoundError(f"Staging videos file not found: {staging_videos_path}")

    print(f"[warehouse] Reading {staging_channels_path}")
    ch = pd.read_parquet(staging_channels_path)

    print(f"[warehouse] Reading {staging_videos_path}")
    vd = pd.read_parquet(staging_videos_path)

    # Ensure snapshot_date is datetime.date
    ch["snapshot_date"] = pd.to_datetime(ch["snapshot_date"]).dt.date
    vd["snapshot_date"] = pd.to_datetime(vd["snapshot_date"]).dt.date

    warehouse_dir = _ensure_warehouse_dir()

    # 1. dim_channel: one row per channel, latest snapshot
    ch_latest = (
        ch.sort_values("snapshot_date")
        .drop_duplicates(subset=["channel_id"], keep="last")
        .reset_index(drop=True)
    )
    ch_latest.insert(0, "channel_key", range(1, len(ch_latest) + 1))

    dim_channel = ch_latest[
        [
            "channel_key",
            "channel_id",
            "channel_title",
            "channel_description",
            "channel_published_at",
            "country",
            "uploads_playlist_id",
        ]
    ]

    dim_channel_path = warehouse_dir / "dim_channel.parquet"
    dim_channel.to_parquet(dim_channel_path, index=False)
    print(f"[warehouse] Wrote dim_channel ({len(dim_channel)} rows) to {dim_channel_path}")

    # 2. dim_video: one row per video, latest snapshot, with channel_key
    vd_latest = (
        vd.sort_values("snapshot_date")
        .drop_duplicates(subset=["video_id"], keep="last")
        .reset_index(drop=True)
    )

    vd_latest = vd_latest.merge(
        dim_channel[["channel_key", "channel_id"]],
        on="channel_id",
        how="left",
    )

    vd_latest.insert(0, "video_key", range(1, len(vd_latest) + 1))

    dim_video = vd_latest[
        [
            "video_key",
            "video_id",
            "channel_key",
            "video_title",
            "video_description",
            "published_at",
            "category_id",
            "duration_seconds",
            "definition",
            "caption",
        ]
    ]

    dim_video_path = warehouse_dir / "dim_video.parquet"
    dim_video.to_parquet(dim_video_path, index=False)
    print(f"[warehouse] Wrote dim_video ({len(dim_video)} rows) to {dim_video_path}")

    # 3. fct_channel_daily_stats: one row per channel per snapshot_date
    fct_channel = ch.merge(
        dim_channel[["channel_key", "channel_id"]],
        on="channel_id",
        how="left",
    )

    fct_channel = fct_channel[
        [
            "snapshot_date",
            "channel_key",
            "view_count",
            "subscriber_count",
            "video_count",
        ]
    ].sort_values(["channel_key", "snapshot_date"])

    fct_channel_path = warehouse_dir / "fct_channel_daily_stats.parquet"
    fct_channel.to_parquet(fct_channel_path, index=False)
    print(
        f"[warehouse] Wrote fct_channel_daily_stats "
        f"({len(fct_channel)} rows) to {fct_channel_path}"
    )

    # 4. fct_video_daily_stats: one row per video per snapshot_date
    fct_video = vd.merge(
        dim_video[["video_key", "video_id"]],
        on="video_id",
        how="left",
    )

    fct_video = fct_video[
        [
            "snapshot_date",
            "video_key",
            "view_count",
            "like_count",
            "comment_count",
            "favorite_count",
        ]
    ].sort_values(["video_key", "snapshot_date"])

    fct_video_path = warehouse_dir / "fct_video_daily_stats.parquet"
    fct_video.to_parquet(fct_video_path, index=False)
    print(
        f"[warehouse] Wrote fct_video_daily_stats "
        f"({len(fct_video)} rows) to {fct_video_path}"
    )


if __name__ == "__main__":
    build_warehouse()
