from googleapiclient.discovery import build # type: ignore
from dotenv import load_dotenv
import os


def get_youtube_client():
    """
    Create and return an authenticated YouTube Data API client.
    Reads the API key from the YT_API_KEY environment variable or .env file.
    """
    # Load .env file once
    load_dotenv()

    api_key = os.getenv("YT_API_KEY")
    if not api_key:
        raise RuntimeError(
            "YT_API_KEY is not set. Add it to your .env file in the project root."
        )

    youtube = build("youtube", "v3", developerKey=api_key)
    return youtube
