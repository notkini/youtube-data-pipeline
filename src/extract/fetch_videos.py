from pathlib import Path
from datetime import date
import json
from typing import List

from utils.youtube_client import get_youtube_client


def chunk_list(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def get_uploads_playlist_id(youtube, channel_id: str) -> str | None:
    """Return the uploads playlist ID for a channel."""
    request = youtube.channels().list(
        part="contentDetails",
        id=channel_id,
    )
    response = request.execute()
    items = response.get("items", [])
    if not items:
        print(f"[videos] No channel found for id={channel_id}")
        return None

    content_details = items[0].get("contentDetails", {})
    playlists = content_details.get("relatedPlaylists", {})
    uploads_id = playlists.get("uploads")
    if not uploads_id:
        print(f"[videos] No uploads playlist for channel {channel_id}")
    return uploads_id


def get_all_video_ids_from_playlist(youtube, playlist_id: str, max_videos: int | None = None) -> List[str]:
    """
    Get all video IDs from an uploads playlist.

    If max_videos is set, stop after that many.
    """
    video_ids: list[str] = []
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token,
        )
        response = request.execute()
        items = response.get("items", [])

        for item in items:
            content = item.get("contentDetails", {})
            vid = content.get("videoId")
            if vid:
                video_ids.append(vid)
                if max_videos is not None and len(video_ids) >= max_videos:
                    return video_ids

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return video_ids


def fetch_video_details(youtube, video_ids: List[str]) -> List[dict]:
    """Fetch full video details for a list of video IDs."""
    all_items: list[dict] = []

    for batch in chunk_list(video_ids, 50):  # API limit 50 ids per call
        request = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(batch),
        )
        response = request.execute()
        items = response.get("items", [])
        all_items.extend(items)

    return all_items


def fetch_videos_for_channels(
    channel_ids: List[str],
    run_date: str | None = None,
    max_videos_per_channel: int | None = None,
) -> Path:
    """
    For each channel, fetch all its videos and save raw JSON.

    Parameters
    ----------
    channel_ids : list of channel IDs
    run_date    : YYYY-MM-DD, defaults to today
    max_videos_per_channel : optional limit to avoid huge downloads

    Returns
    -------
    Path to the written JSON file
    """
    if not channel_ids:
        raise ValueError("channel_ids list is empty")

    if run_date is None:
        run_date = date.today().isoformat()

    youtube = get_youtube_client()

    all_video_items: list[dict] = []

    for channel_id in channel_ids:
        print(f"[videos] Processing channel {channel_id}")

        uploads_playlist_id = get_uploads_playlist_id(youtube, channel_id)
        if not uploads_playlist_id:
            continue

        video_ids = get_all_video_ids_from_playlist(
            youtube,
            uploads_playlist_id,
            max_videos=max_videos_per_channel,
        )
        print(f"[videos] Found {len(video_ids)} videos for channel {channel_id}")

        if not video_ids:
            continue

        video_items = fetch_video_details(youtube, video_ids)
        print(f"[videos] Retrieved details for {len(video_items)} videos for channel {channel_id}")

        all_video_items.extend(video_items)

    # Output
    output_dir = Path("data") / "raw" / "videos" / f"run_date={run_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "videos.json"

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(all_video_items, f, ensure_ascii=False, indent=2)

    print(f"[videos] Saved {len(all_video_items)} videos to {output_path}")
    return output_path
