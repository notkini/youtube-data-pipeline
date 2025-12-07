from pathlib import Path
from datetime import date
import json
from typing import List

from utils.youtube_client import get_youtube_client


def chunk_list(items: List[str], size: int) -> List[List[str]]:
    """Split a list into chunks of max length size."""
    return [items[i:i + size] for i in range(0, len(items), size)]


def fetch_channels(channel_ids: List[str], run_date: str | None = None) -> Path:
    """
    Fetch channel details for the given channel IDs and save raw JSON.

    Parameters
    ----------
    channel_ids : list of YouTube channel IDs
    run_date    : optional run date in YYYY-MM-DD format. Defaults to today.

    Returns
    -------
    Path to the written JSON file.
    """
    if not channel_ids:
        raise ValueError("channel_ids list is empty")

    if run_date is None:
        run_date = date.today().isoformat()

    youtube = get_youtube_client()

    all_items: list[dict] = []

    # YouTube API limit is 50 channel IDs per request
    for batch in chunk_list(channel_ids, 50):
        request = youtube.channels().list(
            part="snippet,statistics,contentDetails",
            id=",".join(batch),
        )
        response = request.execute()
        items = response.get("items", [])
        all_items.extend(items)

    # Prepare output path
    output_dir = Path("data") / "raw" / "channels" / f"run_date={run_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "channels.json"

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(all_items, f, ensure_ascii=False, indent=2)

    print(f"[channels] Saved {len(all_items)} channels to {output_path}")
    return output_path
