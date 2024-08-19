from pyspark.sql import SparkSession
from src.utils.helper import load_log
from datetime import datetime

def transform_user_type(spark, df):

    current_timestamp = datetime.now()
    try:
        # rename column user_type_id to user_type_nk
        df = df.withColumnRenamed("user_type_id", "user_type_nk")

        # drop column created_at
        df = df.drop("created_at")

        #log message
        log_msg = spark.sparkContext\
            .parallelize([("warehouse", "transform", "success", "staging", "user_type", current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
        
        return df
    except Exception as e:
        print(e)

        # log message
        log_msg = spark.sparkContext\
            .parallelize([("warehouse", "transform", "failed", "staging", "user_type", current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])

    finally:
        # load log
        load_log(spark, log_msg)