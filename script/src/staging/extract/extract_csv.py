from pyspark.sql import SparkSession
from src.utils.helper import load_log
from datetime import datetime

PATH = "data/"

def extract_csv(spark: SparkSession, file_name):

    current_timestamp = datetime.now()

    try:

        df = spark.read.csv(PATH + file_name, header=True)

        # log message
        log_msg = spark.sparkContext\
            .parallelize([("staging", "extraction", "success", "csv", file_name, current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
        
        return df
    except Exception as e:
        print(e)

        # log message
        log_msg = spark.sparkContext\
            .parallelize([("staging", "extraction", "failed", "csv", file_name, current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
    finally:
        # load log
        load_log(spark, log_msg)