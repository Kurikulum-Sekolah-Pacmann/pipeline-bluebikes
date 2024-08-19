from pyspark.sql import SparkSession
from src.utils.helper import load_log
from datetime import datetime

def transform_station(spark, df):
    current_timestamp = datetime.now()

    try:
        # rename column station_id to station_nk
        df = df.withColumnRenamed("station_id", "station_nk")

        # drop column created_at
        df = df.drop("created_at")
      
        #log message
        log_msg = spark.sparkContext\
            .parallelize([("warehouse", "transform", "success", "staging", "station", current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
        
        return df
    except Exception as e:
        print(e)

        # log message
        log_msg = spark.sparkContext\
            .parallelize([("warehouse", "transform", "failed", "staging", "station", current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
    finally:
        # load log
        load_log(spark, log_msg)