from pathlib import Path
from datetime import datetime
import json

import pandas as pd
import isodate  # type: ignore


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


def _parse_duration_seconds(duration_str: str | None) -> float | None:
    if not duration_str:
        return None
    try:
        return isodate.parse_duration(duration_str).total_seconds()
    except Exception:
        return None


def transform_videos() -> Path:
    """
    Transform raw video JSON into a clean tabular format and save as Parquet.

    Reads from:
        data/raw/videos/run_date=YYYY-MM-DD/videos.json

    Writes to:
        data/staging/videos/videos.parquet

    Returns
    -------
    Path to the Parquet file.
    """
    raw_root = Path("data") / "raw" / "videos"
    latest_dir, run_date = _get_latest_run_dir(raw_root)
    raw_file = latest_dir / "videos.json"

    if not raw_file.exists():
        raise FileNotFoundError(f"Raw videos file not found: {raw_file}")

    print(f"[transform_videos] Reading {raw_file}")

    with raw_file.open("r", encoding="utf-8") as f:
        raw_items = json.load(f)

    rows: list[dict] = []
    snapshot_date = datetime.strptime(run_date, "%Y-%m-%d").date()

    for item in raw_items:
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        content = item.get("contentDetails", {})

        duration_seconds = _parse_duration_seconds(content.get("duration"))

        rows.append(
            {
                "video_id": item.get("id"),
                "channel_id": snippet.get("channelId"),
                "video_title": snippet.get("title"),
                "video_description": snippet.get("description"),
                "published_at": snippet.get("publishedAt"),
                "category_id": snippet.get("categoryId"),
                "duration_seconds": duration_seconds,
                "definition": content.get("definition"),
                "caption": content.get("caption"),
                "licensed_content": content.get("licensedContent"),
                "view_count": int(stats.get("viewCount", 0)) if stats.get("viewCount") is not None else None,
                "like_count": int(stats.get("likeCount", 0)) if stats.get("likeCount") is not None else None,
                "favorite_count": int(stats.get("favoriteCount", 0)) if stats.get("favoriteCount") is not None else None,
                "comment_count": int(stats.get("commentCount", 0)) if stats.get("commentCount") is not None else None,
                "snapshot_date": snapshot_date,
            }
        )

    df = pd.DataFrame(rows)

    # Parse dates
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

    # Output path
    out_dir = Path("data") / "staging" / "videos"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "videos.parquet"

    df.to_parquet(out_path, index=False)
    print(f"[transform_videos] Wrote {len(df)} rows to {out_path}")

    return out_path


if __name__ == "__main__":
    transform_videos()
