# Function Extarct with log
from src.utils.helper import load_log, bluebikes_engine
from datetime import datetime
from pyspark.sql import SparkSession

def extract_database(spark: SparkSession, table_name):
    # get config
    DB_URL, DB_USER, DB_PASS = bluebikes_engine()

    # set config
    connection_properties = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver" # set driver postgres
    }

    current_timestamp = datetime.now()
    
    try:
        # read data
        df = spark \
                .read \
                .jdbc(url = DB_URL,
                        table = table_name,
                        properties = connection_properties)
    
        # log message
        log_msg = spark.sparkContext\
            .parallelize([("staging", "extraction", "success", "db_bluebikes", table_name, current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
        
        return df
    except Exception as e:
        print(e)

        # log message
        log_msg = spark.sparkContext\
            .parallelize([("staging", "extraction", "failed", "db_bluebikes", table_name, current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
    finally:
        # load log
        load_log(spark, log_msg)