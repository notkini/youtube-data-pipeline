import duckdb
from pathlib import Path

WAREHOUSE_DIR = Path("data") / "warehouse"
SQL_ANALYSIS_DIR = Path("sql") / "analysis"


def get_connection():
    # In memory DB that reads Parquet directly
    con = duckdb.connect(database=":memory:")

    # Register Parquet files as views
    con.execute(f"""
        CREATE VIEW dim_channel AS
        SELECT * FROM '{WAREHOUSE_DIR / "dim_channel.parquet"}';
    """)

    con.execute(f"""
        CREATE VIEW dim_video AS
        SELECT * FROM '{WAREHOUSE_DIR / "dim_video.parquet"}';
    """)

    con.execute(f"""
        CREATE VIEW fct_channel_daily_stats AS
        SELECT * FROM '{WAREHOUSE_DIR / "fct_channel_daily_stats.parquet"}';
    """)

    con.execute(f"""
        CREATE VIEW fct_video_daily_stats AS
        SELECT * FROM '{WAREHOUSE_DIR / "fct_video_daily_stats.parquet"}';
    """)

    return con


def run_sql_file(con: duckdb.DuckDBPyConnection, filename: str):
    sql_path = SQL_ANALYSIS_DIR / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    with sql_path.open("r", encoding="utf-8") as f:
        query = f.read()

    print(f"\nRunning query from {sql_path}...")
    df = con.execute(query).fetch_df()
    print(df.head(20))
    print(f"Total rows: {len(df)}")


def main():
    con = get_connection()

    print("Tables available: dim_channel, dim_video, fct_channel_daily_stats, fct_video_daily_stats")

    # Run your analysis queries
    run_sql_file(con, "top_videos.sql")
    run_sql_file(con, "growth_analysis.sql")
    run_sql_file(con, "upload_strategy.sql")


if __name__ == "__main__":
    main()
