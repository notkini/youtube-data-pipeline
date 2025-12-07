# YouTube Data Engineering & Analytics Pipeline

This project is an end to end data engineering and analytics pipeline built using the YouTube Data API. The goal of this project is to ingest, process, store, and analyze YouTube channel and video data at scale.

 **Day 1 Status: Raw Data Ingestion Completed**

---

## Project Goals

- Build a fully automated YouTube data pipeline
- Store raw API data in a structured data lake format
- Create a scalable data warehouse for analytics
- Perform time series and engagement analysis
- Build an interactive analytics dashboard

---

## Tech Stack

- Python
- YouTube Data API v3
- Google API Python Client
- SQL (PostgreSQL planned)
- Pandas (upcoming)
- Apache Airflow (planned)
- Power BI / Tableau / Streamlit (planned)

---

## Pipeline Architecture (Current Stage)

YouTube API  
→ Python Extraction Scripts  
→ Raw JSON Storage (Data Lake)

---

## Tracked YouTube Channels

The project currently tracks the following channels:

- Google Developers  
- freeCodeCamp  
- Fireship  
- Lex Fridman  
- CNBC  

These channels were selected to allow cross-domain analysis across:
- Education
- Software Development
- AI
- Podcasts
- Business News

---

## Day 1: Raw Data Ingestion

### What Was Implemented

- Secure API authentication using environment variables
- Multi-channel YouTube data extraction
- Channel metadata ingestion
- Full historical video metadata ingestion
- Batch-based API fetching to stay within quota limits
- Raw data stored in partitioned data lake structure

---

## Raw Data Storage Structure

```text
data/
└── raw/
    ├── channels/
    │   └── run_date=YYYY-MM-DD/
    │       └── channels.json
    └── videos/
        └── run_date=YYYY-MM-DD/
            └── videos.json

**How to Run the Day 1 Pipeline**
1. Clone the repository
git clone https://github.com/YOUR_USERNAME/youtube-data-pipeline.git
cd youtube-data-pipeline

2. Create and activate virtual environment
python -m venv venv
venv\Scripts\activate

3. Install dependencies
pip install google-api-python-client python-dotenv

4. Add YouTube API Key
Create a .env file in the root directory:
YT_API_KEY=YOUR_API_KEY_HERE

5. Run the pipeline
python src/main.py

