# Project Summary
The project sets up an ETL (Extract, Transform, Load) environment using Docker, Airflow, and PostgreSQL to create a data pipeline from an API of breweries (OpenBreweryDB) into a data lake with bronze, silver, and gold layers. 

# Execution

## Requirements
- Docker
- Docker Compose
- Setup Instructions

### Clone the Repository:
```sh
git clone https://github.com/aristotelesbelarmino/abinbev_case.git
cd abinbev_case
```

### Build and Start Docker Containers:
```sh
docker-compose up --build -d
```
### Access Airflow UI:
- Open a web browser and go to http://localhost:8080
- Use Airflow's default credentials (username: airflow, password: airflow) to login.

### Run the Airflow DAG:
- Navigate to the Airflow UI and enable the extract_api_data DAG.
- Trigger the DAG to start processing data from the OpenBreweryDB API.
- Monitor the DAG's progress in the Airflow UI.

# Key Files and Structure
### Dockerfile (Dockerfile.airflow):
  - Sets up the Docker environment for Airflow, installing dependencies like pyarrow and pandas required for data processing.

### docker-compose.yml:
  - Defines Docker services for Airflow and PostgreSQL.
  - Configures volumes for data persistence and initialization of SQL scripts.
### Notes
Adjust configurations and environment variables (docker-compose.yml, Dockerfile.airflow, etc.) as needed for specific setups.
Ensure Docker and Docker Compose are properly configured and running on your system.

### SQL Scripts (sql_scripts/):
  - 00_init.sql: Initializes the PostgreSQL database and executes subsequent scripts.
  - 01_create_bronze.sql, 02_create_silver.sql, 03_create_gold.sql, 04_create_agg.sql: Creates tables for bronze, silver, gold layers, and aggregation.

### DAGs (dags/):
  - extract_api_data.py: Airflow DAG to extract data from the OpenBreweryDB API into the bronze layer, transform it into the silver layer, and then aggregate it into the gold layer.
  - dw_etl/:
    - dw_bronze_layer.py: Extracts data from the API, saves JSON files in the bronze layer, and inserts raw data into the bronze_breweries table.
    - dw_silver_layer.py: Transforms bronze layer data into Parquet format in the silver layer, organizing by state (state) using Pandas and PyArrow.
    - dw_gold_layer.py: Reads data from the silver layer, performs aggregations (count of breweries by type and state), and inserts results into the gold layer in PostgreSQL.

## Overall Functionality
  - Extraction (Bronze Layer): Uses the OpenBreweryDB API to fetch brewery data, saving JSON files in the bronze layer and inserting raw data into the bronze_breweries table.
  - Transformation (Silver Layer): Transforms data from the bronze layer into Parquet files in the silver layer, organized by state (state), utilizing Pandas and PyArrow.
  - Loading (Gold Layer): Reads transformed data from the silver layer, performs aggregations (count of breweries by type and state), and inserts results into the gold layer in the aggregated_breweries table.

## Execution and Integration
Airflow DAG (extract_api_data): Defines sequential tasks to extract, transform, and load data.
  - extract_data: Task to extract data from the API and save it in the bronze layer.
  - transform_to_silver: Task to transform bronze layer data into Parquet files in the silver layer.
  - transform_to_gold: Task to aggregate silver layer data and insert it into the gold layer.


## How could we keep it running smoothly?
### 1. Data Quality Checks:

  - Validate data integrity to ensure completeness and accuracy.
  - Example: Detect null values or duplicates in critical data fields.

### 2. Pipeline Monitoring:

  - Continuous monitoring of data flow to detect and respond to failures promptly.
  - Example: Monitor pipeline stages for errors and performance metrics.


### 3. Integration with Airflow:

  - Using Airflow for scheduling and orchestrating pipeline tasks.
  - Example: Define workflows and dependencies to automate data processing.


### 4. Observability Tools:

  - Utilize Prometheus and Grafana for real-time visibility into pipeline metrics.
  - Example: Visualize data throughput, error rates, and latency for proactive monitoring.

### 5. Improvement:
  - Starting from the file dags/dw_etl/ad-hoc/dw_gold_layer.py, we can build a relational base with the registration data we look for in the API, making dedicated queries more efficient!