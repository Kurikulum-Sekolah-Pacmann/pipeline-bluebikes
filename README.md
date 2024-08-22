# Project Name: BlueBikes Data Pipeline

## Description
This project aims to build a data pipeline for processing BlueBikes data. BlueBikes is a bike-sharing system in the Greater Boston area. The pipeline will collect, transform, and load the data into a database for further analysis.

## Features
- Data collection from BlueBikes 
- Data transformation and cleaning
- Database integration for storing processed data

## Installation
1. Clone the repository: `git clone https://github.com/your-username/bluebikes-data-pipeline.git`
  - Build: docker compose up --build --detach
  - Copy Driver: docker cp driver/postgresql-42.6.0.jar pyspark_container2:/usr/local/spark/jars/postgresql-42.6.0.jar
2. Copy data from  **Bluebikes Trip Data CSV**: [Link to Dataset](https://www.kaggle.com/datasets/jackdaoud/bluebikes-in-boston)
to directory: ./script/data
3. create your .env
``` 
  DB_HOST="YOUR HOST"
  DB_USER="USERNAME"
  DB_PASS="PASS"
  DB_PORT="PORT"
  DB_NAME_CLINIC="clinic"
  DB_NAME_CLINIC_OPS="clinic_ops"
  DB_NAME_STG="staging_clinic"
  DB_NAME_LOG="etl_log"
  DB_NAME_WH="warehouse_clinic"
  CRED_PATH='your_path/creds/creds.json'
  KEY_SPREADSHEET='YOUR KEY SPREADSHEET'
  ACCESS_KEY_MINIO = 'ACCESS KEY MINIO'
  SECRET_KEY_MINIO = 'SECRET KEY MINIO'
  MODEL_PATH_LOG_ETL='your_path/src/utils/model/'
```

## Usage
1. Access the terminal of the container: `docker exec -it pyspark_container2 /bin/bash `
2. Navigate to the project directory: `/home/jovyan/work`
3. Run the pipeline script: `spark-submit _pipeline.py`

alternative:
1. Access the Jupyter Notebook server at:: [localhost:8888](http://localhost:8888/)
2. Run Notebook live_w7.ipynb