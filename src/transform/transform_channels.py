from pathlib import Path
from datetime import datetime
import json

import pandas as pd


def _get_latest_run_dir(base_path: Path) -> tuple[Path, str]:
    """
    Find the latest 'run_date=YYYY-MM-DD' directory under base_path.

    Returns (path, run_date_str).
    """
    if not base_path.exists():
        raise FileNotFoundError(f"Base path does not exist: {base_path}")

    run_dirs = [
        p for p in base_path.iterdir()
        if p.is_dir() and p.name.startswith("run_date=")
    ]
    if not run_dirs:
        raise FileNotFoundError(f"No run_date=... folders found under {base_path}")

    latest_dir = max(run_dirs, key=lambda p: p.name)
    run_date = latest_dir.name.split("=", 1)[1]
    return latest_dir, run_date


def transform_channels() -> Path:
    """
    Transform raw channel JSON into a clean tabular format and save as Parquet.

    Reads from:
        data/raw/channels/run_date=YYYY-MM-DD/channels.json

    Writes to:
        data/staging/channels/channels.parquet

    Returns
    -------
    Path to the Parquet file.
    """
    raw_root = Path("data") / "raw" / "channels"
    latest_dir, run_date = _get_latest_run_dir(raw_root)
    raw_file = latest_dir / "channels.json"

    if not raw_file.exists():
        raise FileNotFoundError(f"Raw channels file not found: {raw_file}")

    print(f"[transform_channels] Reading {raw_file}")

    with raw_file.open("r", encoding="utf-8") as f:
        raw_items = json.load(f)

    rows: list[dict] = []
    snapshot_date = datetime.strptime(run_date, "%Y-%m-%d").date()

    for item in raw_items:
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        content = item.get("contentDetails", {})
        related_playlists = content.get("relatedPlaylists", {})

        rows.append(
            {
                "channel_id": item.get("id"),
                "channel_title": snippet.get("title"),
                "channel_description": snippet.get("description"),
                "channel_published_at": snippet.get("publishedAt"),
                "country": snippet.get("country"),
                "view_count": int(stats.get("viewCount", 0)) if stats.get("viewCount") is not None else None,
                "subscriber_count": int(stats.get("subscriberCount", 0)) if stats.get("subscriberCount") is not None else None,
                "hidden_subscriber_count": stats.get("hiddenSubscriberCount"),
                "video_count": int(stats.get("videoCount", 0)) if stats.get("videoCount") is not None else None,
                "uploads_playlist_id": related_playlists.get("uploads"),
                "snapshot_date": snapshot_date,
            }
        )

    df = pd.DataFrame(rows)

    # Parse dates
    df["channel_published_at"] = pd.to_datetime(
        df["channel_published_at"], errors="coerce"
    )

    # Output path
    out_dir = Path("data") / "staging" / "channels"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "channels.parquet"

    df.to_parquet(out_path, index=False)
    print(f"[transform_channels] Wrote {len(df)} rows to {out_path}")

    return out_path


if __name__ == "__main__":
    transform_channels()
