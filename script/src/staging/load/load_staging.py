from src.utils.helper import load_log, stg_engine  
from datetime import datetime
from pyspark.sql import SparkSession

def load_staging(spark: SparkSession, df, table_name, source_name):
    current_timestamp = datetime.now()
    DB_URL, DB_USER, DB_PASS = stg_engine()
    properties = {
    "user": DB_USER,
    "password": DB_PASS
    }
    try:
        df.write.jdbc(url = DB_URL,
                    table = table_name,
                    mode = "overwrite",
                    properties = properties)
        
        #log message
        log_msg = spark.sparkContext\
            .parallelize([("staging", "load", "success", source_name, table_name, current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
        
    except Exception as e:
        print(e)
        
        # log message
        log_msg = spark.sparkContext\
            .parallelize([("staging", "load", "success", source_name, table_name, current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
        
    finally:
        load_log(spark, log_msg)