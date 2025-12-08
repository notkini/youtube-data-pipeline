from datetime import date

from extract.fetch_channels import fetch_channels
from extract.fetch_videos import fetch_videos_for_channels
from transform.transform_channels import transform_channels
from transform.transform_videos import transform_videos
from load.load_to_warehouse import build_warehouse

# Put the channel IDs you want to track here
CHANNEL_IDS = [
    "UC_x5XG1OV2P6uZZ5FSM9Ttw",  # Google Developers
    "UC8butISFwT-Wl7EV0hUK0BQ",  # freeCodeCamp
    "UCsBjURrPoezykLs9EqgamOA",  # Fireship
    "UCSHZKyawb77ixDdsGog4iWA",  # Lex Fridman
    "UCvJJ_dzjViJCoLf5uKUTwoA",  # CNBC
]


def main():
    if not CHANNEL_IDS:
        raise RuntimeError(
            "CHANNEL_IDS is empty. Add at least one YouTube channel ID in src/main.py."
        )

    run_date = date.today().isoformat()
    print(f"Starting YouTube pipeline for run_date={run_date}")

    # Day 1: raw ingestion
    channels_path = fetch_channels(CHANNEL_IDS, run_date=run_date)
    videos_path = fetch_videos_for_channels(
        CHANNEL_IDS,
        run_date=run_date,
        max_videos_per_channel=None,  # you are using full data
    )

    print("Raw ingestion completed.")
    print(f"Channels raw file: {channels_path}")
    print(f"Videos raw file:   {videos_path}")

    # Day 2: transformations
    staging_channels_path = transform_channels()
    staging_videos_path = transform_videos()

    print("Staging transformation completed.")
    print(f"Channels staging file: {staging_channels_path}")
    print(f"Videos staging file:   {staging_videos_path}")

    # Day 2: warehouse build
    build_warehouse()
    print("Warehouse build completed.")


if __name__ == "__main__":
    main()
