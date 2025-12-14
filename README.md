**YouTube Data Engineering Pipeline**

**Title**
A complete end to end data pipeline that ingests multi channel YouTube data, builds a structured data lake, transforms JSON to Parquet, constructs a star schema warehouse, and enables analytics using DuckDB SQL




**Architecture Diagram**

<img width="1092" height="3936" alt="image" src="https://github.com/user-attachments/assets/c7aa7a5f-19dc-4252-aad0-03b2af41d70e" />

**Features**

1)Automated ingestion of YouTube channel + video metadata

2)Historical raw data stored with date partitions

3)Data transformation from nested JSON into clean analytical tables

4)Warehouse modeled with dimensions and fact tables

5)Parquet optimized storage

6)SQL analytics with DuckDB

7)Modular and scalable codebase

**Data Pipeline Flow**

**Extraction**

Fetches channel metadata

Fetches video metadata

Saves raw JSON to data/raw/run_date=YYYY-MM-DD

**Transformation**

Normalizes nested JSON

Parses ISO8601 durations

Produces staging Parquet tables

**Warehouse**

Star schema design

Dimension tables: dim_channel, dim_video

Fact tables: fct_channel_daily_stats, fct_video_daily_stats

Stored in data/warehouse/ as Parquet

**Analytics**

DuckDB SQL queries

Growth analysis

Top videos

Upload strategy insights

**Setup instructions**

git clone <repo>
cd youtube-data-pipeline

python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt

**Add your YouTube API Key:**

Create .env:

YT_API_KEY=YOUR_API_KEY

**Running the Pipeline**

**Run Full Pipeline**
python src/main.py

**Run Analytics**

python src/analysis_run.py

**Generate BI CSVs**

python src/create_bi_csvs.py

**Future Enhancements**

Deploy pipeline to AWS (S3 + Glue + Athena)

Add Airflow orchestration

Add monitoring & logging

Build BI dashboard (Tableau, Power BI, or Streamlit)

**Contact**

Author: Aditya Kinikar
Feel free to reach out on GitHub or LinkedIn.
