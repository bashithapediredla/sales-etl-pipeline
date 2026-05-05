# Crypto Data Pipeline (API → Azure → Database)

## Overview
This project implements an end-to-end data pipeline that extracts real-time cryptocurrency data from an API, stores raw data in Azure Blob Storage, processes it using Python, and loads it into a structured SQLite database for querying.

The pipeline simulates a real-world data engineering workflow with cloud storage, transformation, and incremental data ingestion.

---

## Pipeline Architecture
API → Azure Blob Storage → Python Processing → SQLite Database → Query

---

## Features
- Real-time data extraction from public API
- Cloud storage using Azure Blob Storage
- Data transformation and timestamping
- Incremental data insertion into database
- Secure configuration using environment variables (.env)
- Error handling for API failures

---

## Key Design Decisions
- Used Azure Blob Storage to simulate a raw data layer
- Separated configuration using `.env` to avoid hardcoding secrets
- Added timestamps to track time-series data
- Combined scripts into a single orchestrated pipeline

---

## Sample Output
(1, 67000, '2026-05-05 12:30:00')  
(2, 67000, '2026-05-05 12:35:00')

---

## Tech Stack
- Python
- SQLite
- Azure Blob Storage
- REST API
- python-dotenv

---

## Future Improvements
- Automate pipeline using scheduler (cron/Airflow)
- Extend to multiple cryptocurrencies
- Replace SQLite with cloud database
- Add data validation layer

---

## How to Run
pip install requests azure-storage-blob python-dotenv  
python pipeline.py