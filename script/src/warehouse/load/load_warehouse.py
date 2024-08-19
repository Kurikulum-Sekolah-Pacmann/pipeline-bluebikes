from src.utils.helper import load_log, wh_engine, wh_engine_sqlalchemy
from datetime import datetime
from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text



# before load to warehouse, truncate the table with sqlalchemy
def load_warehouse(spark: SparkSession, df, table_name, source_name):
    current_timestamp = datetime.now()
    DB_URL, DB_USER, DB_PASS = wh_engine()
    properties = {
    "user": DB_USER,
    "password": DB_PASS
    }
    try:
        # truncate table with sqlalchemy
        conn = wh_engine_sqlalchemy()

        with conn.connect() as connection:
            # Execute the TRUNCATE TABLE command
            connection.execute(text(f"TRUNCATE TABLE {table_name} CASCADE RESTART IDENTITY"))
            connection.commit()
            connection.close()
        conn.dispose()
    except Exception as e:
        log_msg = spark.sparkContext\
            .parallelize([("warehouse", "load", "failed", source_name, table_name, current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
        load_log(spark, log_msg)
    
    try:
        # load data
        df.write.jdbc(url = DB_URL,
                    table = table_name,
                    mode = "append",
                    properties = properties)
        
        #log message
        log_msg = spark.sparkContext\
            .parallelize([("warehouse", "load", "success", source_name, table_name, current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
        
    except Exception as e:
        # print(e)
        
        # log message
        log_msg = spark.sparkContext\
            .parallelize([("warehouse", "load", "failed", source_name, table_name, current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
        
    finally:
        load_log(spark, log_msg)