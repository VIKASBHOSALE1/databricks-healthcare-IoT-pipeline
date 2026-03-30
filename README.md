
End-to-end healthcare IoT data pipeline using Databricks, Delta Lake, Structured Streaming, and Medallion Architecture

# databricks-healthcare-IoT-pipeline
End-to-end healthcare IoT data pipeline using Databricks, Delta Lake, Structured Streaming, and Medallion Architecture	

# Healthcare IoT Patient Vitals Monitoring Pipeline
 
## What this project does
An end-to-end data engineering pipeline that ingests real-time IoT patient
vitals data, processes it through a Medallion Architecture (Bronze ? Silver ? Gold),
detects anomalies, and serves analytics via a SQL Dashboard — built entirely
on Databricks Community Edition (zero cost).
 
=======
<img width="1765" height="902" alt="image" src="https://github.com/user-attachments/assets/ee3f0e9d-cd82-4f40-a7a5-44ad13c8c436" />
<img width="1485" height="841" alt="image" src="https://github.com/user-attachments/assets/a45e08f6-22a9-4ab3-b887-0690d6ae53e8" />





## What this project does
An end-to-end data engineering pipeline that ingests real-time IoT patient
vitals data, processes it through a Medallion Architecture (Bronze ? Silver ? Gold),
detects anomalies, and serves analytics via a SQL Dashboard â€” built entirely
on Databricks Community Edition (zero cost).
 
## Architecture




 
## Tech Stack
| Tool              | Purpose                                      |
|-------------------|----------------------------------------------|
| Databricks CE     | Compute, notebooks, SQL dashboard            |
| Delta Lake 3.x    | ACID storage, time travel, schema evolution  |
| Auto Loader       | Incremental file ingestion                   |
| Structured Stream | Real-time micro-batch processing             |
| Unity Catalog     | Data governance and table registry           |
| PySpark 3.5       | Transformations and aggregations             |
 


## Pipeline Phases
| Phase   | Notebook                  | Description                          |
|---------|---------------------------|--------------------------------------|
| Phase 1 | 01_data_simulator.py      | Reads CSV, splits into JSON batches  |
| Phase 2 | 02_bronze_ingestion.py    | Auto Loader ? Bronze Delta table     |
| Phase 3 | 03_silver_cleansing.py    | MERGE, dedup, anomaly detection      |
| Phase 4 | 04_gold_aggregation.py    | Business aggregations + OPTIMIZE     |
| Phase 5 | 05_dashboard_queries.sql  | Databricks SQL Dashboard             |
 
 
## Tech Stack
| Tool              | Purpose                                      |
|-------------------|----------------------------------------------|
| Databricks CE     | Compute, notebooks, SQL dashboard            |
| Delta Lake 3.x    | ACID storage, time travel, schema evolution  |
| Auto Loader       | Incremental file ingestion                   |
| Structured Stream | Real-time micro-batch processing             |
| Unity Catalog     | Data governance and table registry           |
| PySpark 3.5       | Transformations and aggregations             |
 


## Pipeline Phases
| Phase   | Notebook                  | Description                          |
|---------|---------------------------|--------------------------------------|
| Phase 1 | 01_data_simulator.py      | Reads CSV, splits into JSON batches  |
| Phase 2 | 02_bronze_ingestion.py    | Auto Loader ? Bronze Delta table     |
| Phase 3 | 03_silver_cleansing.py    | MERGE, dedup, anomaly detection      |
| Phase 4 | 04_gold_aggregation.py    | Business aggregations + OPTIMIZE     |
| Phase 5 | 05_dashboard_queries.sql  | Databricks SQL Dashboard             |
 


## Key Features Demonstrated
- Medallion Architecture (Bronze / Silver / Gold)
- Structured Streaming with Auto Loader and cloudFiles format
- ACID transactions and Delta Lake time travel (VERSION AS OF)
- MERGE INTO for idempotent deduplication
- Anomaly detection logic for critical patient vitals
- OPTIMIZE with ZORDER BY for query performance
- Unity Catalog with managed Delta tables
- Databricks SQL Dashboard with 5 live widgets
 

## Dataset
Healthcare IoT sensor data — 2200 records across 50 patients, 14 days
Vitals: Heart Rate, SpO2, Temperature, Blood Pressure
Source: Kaggle Healthcare IoT Dataset
 
 

## Dataset
Healthcare IoT sensor data â€” 2200 records across 50 patients, 14 days
Vitals: Heart Rate, SpO2, Temperature, Blood Pressure
Source: Kaggle Healthcare IoT Dataset
 
## How to Run
1. Import notebooks into Databricks Community Edition
2. Create Unity Catalog volumes for landing zone, checkpoints, schemas
3. Upload dataset CSV to landing zone Volume
4. Run notebooks 01 through 05 in sequence
5. Or use the Databricks Job defined in this repo
 
## Dashboard
See outputs/healthcare_iot_dashboard

 
 
## Dashboard
https://dbc-301430ab-feda.cloud.databricks.com/dashboardsv3/01f129f2a5f213a882b12e1a03069e46/published?o=806254623912869

See outputs/<img width="1763" height="887" alt="image" src="https://github.com/user-attachments/assets/8e961233-33ab-463e-ba71-dd4973174b14" />

## Author
Vikas Bhosale | Associate Software Development Engineer 

