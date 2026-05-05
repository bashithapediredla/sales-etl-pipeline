# Sales ETL Pipeline

## Overview
This project demonstrates a simple ETL (Extract, Transform, Load) pipeline using Python.

## Process
- Extract data from CSV
- Transform data (handle missing values, remove duplicates, feature engineering)
- Load into SQLite database
- Perform aggregation queries

## Features
- Data validation
- Incremental loading
- Error handling

## Technologies Used
- Python
- Pandas
- SQLite

## Key Design Decisions

- Missing values handled using mean and default date to maintain data consistency
- Incremental loading implemented to avoid duplicate data insertion
- SQLite used for simplicity and lightweight storage

## Sample Output

Example aggregated result:

| Year | Total Sales |
|------|------------|
| 2024 | 103000     |

## Future Improvements

- Use real-time API data instead of static CSV
- Scale pipeline using distributed tools (e.g., PySpark)
- Add scheduling for automated execution

## How to Run
pip install pandas  
python etl.py

## Output
- Cleaned data
- Total sales by year