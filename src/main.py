from datetime import date

from extract.fetch_channels import fetch_channels
from extract.fetch_videos import fetch_videos_for_channels


# Put the channel IDs you want to track here
# Example: official Google channels
CHANNEL_IDS = [
    "UC_x5XG1OV2P6uZZ5FSM9Ttw",  # Google Developers
    "UC8butISFwT-Wl7EV0hUK0BQ",  #freeCodecamp
    "UCsBjURrPoezykLs9EqgamOA",  #Fireship
    "UCSHZKyawb77ixDdsGog4iWA",  #Lex Fridman
    "UCvJJ_dzjViJCoLf5uKUTwoA"   #CNBC
]


def main():
    if not CHANNEL_IDS:
        raise RuntimeError(
            "CHANNEL_IDS is empty. Add at least one YouTube channel ID in src/main.py."
        )

    run_date = date.today().isoformat()
    print(f"Starting YouTube pipeline for run_date={run_date}")

    # 1. Fetch channel details
    channels_path = fetch_channels(CHANNEL_IDS, run_date=run_date)

    # 2. Fetch video details
    videos_path = fetch_videos_for_channels(
        CHANNEL_IDS,
        run_date=run_date,
        max_videos_per_channel=50,  # set to 50 while testing if you want
    )

    print("Pipeline finished.")
    print(f"Channels raw file: {channels_path}")
    print(f"Videos raw file:   {videos_path}")


if __name__ == "__main__":
    main()
