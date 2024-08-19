from pyspark.sql import SparkSession
from src.utils.helper import load_log
from datetime import datetime

def transform_bike(spark, df):
    current_timestamp = datetime.now()
    try:
        # rename column bike_id to bike_nk
        df = df.withColumnRenamed("bike_id", "bike_nk")

        # drop column created_at
        df = df.drop("created_at")

        #log message
        log_msg = spark.sparkContext\
            .parallelize([("warehouse", "transform", "success", "staging", "bike", current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
        
        return df
    except Exception as e:
        print(e)

        # log message
        log_msg = spark.sparkContext\
            .parallelize([("warehouse", "transform", "failed", "staging", "bike", current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
    finally:
        # load log
        load_log(spark, log_msg)