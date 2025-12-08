from pathlib import Path
import pandas as pd


WAREHOUSE_DIR = Path("data") / "warehouse"
CSV_DIR = WAREHOUSE_DIR / "csv"


def export_parquet_to_csv(parquet_name: str):
    src = WAREHOUSE_DIR / parquet_name
    dst = CSV_DIR / (parquet_name.replace(".parquet", ".csv"))

    if not src.exists():
        raise FileNotFoundError(f"Parquet file not found: {src}")

    print(f"Reading {src}")
    df = pd.read_parquet(src)
    CSV_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Writing {dst}")
    df.to_csv(dst, index=False)


def main():
    files = [
        "dim_channel.parquet",
        "dim_video.parquet",
        "fct_channel_daily_stats.parquet",
        "fct_video_daily_stats.parquet",
    ]

    for fname in files:
        export_parquet_to_csv(fname)

    print("Export completed.")


if __name__ == "__main__":
    main()
