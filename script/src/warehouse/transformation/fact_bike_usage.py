from pyspark.sql import SparkSession
from src.utils.helper import load_log
from datetime import datetime
from pyspark.sql.functions import date_format, col
from src.warehouse.extract.extract_warehouse import extract_warehouse

def transform_fact_bike_usage(spark: SparkSession):
        current_timestamp = datetime.now()

        try:
                # Extarct data fact_trip_data
                columns = ['trip_duration', 'trip_id', 'bike_id']

                df_trip = extract_warehouse(spark, "fact_trip_data", columns)

                # group by bike_id and count trip_id, sum trip_duration
                df_bike_usage = df_trip.groupBy("bike_id") \
                                .agg({"trip_duration": "sum", "trip_id": "count"}) \
                                .withColumnRenamed("sum(trip_duration)", "total_duration") \
                                .withColumnRenamed("count(trip_id)", "trip_count")
                
                #log message
                log_msg = spark.sparkContext\
                .parallelize([("warehouse", "transform", "success", "staging", "fact_bike_usage", current_timestamp)])\
                .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])

                return df_bike_usage
        except Exception as e:
                #log message
                print(e)
                log_msg = spark.sparkContext\
                .parallelize([("warehouse", "transform", "success", "staging", "fact_bike_usage", current_timestamp, str(e))])\
                .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])