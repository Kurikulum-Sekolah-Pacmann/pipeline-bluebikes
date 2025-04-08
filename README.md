# Project Name: BlueBikes Data Pipeline

## Description
This project aims to build a data pipeline for processing BlueBikes data. BlueBikes is a bike-sharing system in the Greater Boston area. The pipeline will collect, transform, and load the data into a database for further analysis.

## Features
- Data collection from BlueBikes 
- Data transformation and cleaning
- Database integration for storing processed data

## Installation
1. Clone the repository: `https://github.com/Kurikulum-Sekolah-Pacmann/pipeline-bluebikes.git`
  - Build: docker compose up --build --detach
  - Copy Driver: docker cp driver/postgresql-42.6.0.jar pyspark_container2:/usr/local/spark/jars/postgresql-42.6.0.jar
2. Copy data from  **Bluebikes Trip Data CSV**: [Link to Dataset](https://www.kaggle.com/datasets/jackdaoud/bluebikes-in-boston)
to directory: ./script/data
3. create your .env
``` 
DB_HOST_SOURCE="CONTAINER NAME"
DB_USER_SOURCE="USERNAME"
DB_PASS_SOURCE="YOUR PASS"
DB_PORT_SOURCE="5432"

DB_HOST_TARGET="CONTAINER NAME"
DB_USER_TARGET="USERNAME"
DB_PASS_TARGET="YOUR PASS"
DB_PORT_TARGET="5432"


DB_NAME_BLUEBIKES="bluebikes"
DB_NAME_STG="staging"
DB_NAME_LOG="etl_log"
DB_NAME_WH="warehouse"
```

## Usage
1. Access the terminal of the container: `docker exec -it pyspark_container2 /bin/bash `
2. Navigate to the project directory: `/home/jovyan/work`
3. Run the pipeline script: `spark-submit _pipeline.py`

alternative:
1. Access the Jupyter Notebook server at:: [localhost:8888](http://localhost:8888/)
2. Run Notebook live_w7.ipynb
