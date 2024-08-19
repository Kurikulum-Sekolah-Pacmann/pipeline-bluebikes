from pyspark.sql import SparkSession
from src.utils.helper import load_log
from datetime import datetime
from pyspark.sql.functions import date_format, col
from src.warehouse.extract.extract_warehouse import extract_warehouse

def transform_fact_trip(spark: SparkSession, df):
    current_timestamp = datetime.now()

    try:
        # Extarct data dimension table
        df_user_type = extract_warehouse(spark, "dim_user_type", ["user_type_id", "user_type_name"])
        df_station = extract_warehouse(spark, "dim_station", ["station_id", "station_nk"])
        df_bike = extract_warehouse(spark, "dim_bike", ["bike_id", "bike_nk"])
        df_date = extract_warehouse(spark, "dim_date", ["date_id", "date_actual"])
        df_time = extract_warehouse(spark, "dim_time", ["time_id", "time_actual"])

        # convert df_time column time_actual to HH:mm:ss
        df_time = df_time.withColumn("time_actual", date_format(col("time_actual"),  "HH:mm:ss"))

        #convert tripduration to int 
        df = df.withColumn("tripduration", df["tripduration"].cast("int"))

        # rename column
        df = df.withColumnRenamed("tripduration", "trip_duration") \
                .withColumnRenamed("start station id", "start_station_nk") \
                .withColumnRenamed("end station id", "end_station_nk") \
                .withColumnRenamed("bikeid", "bike_nk") \
                .withColumnRenamed("usertype", "user_type_name")
    
        
        # extract date and time from starttime and stoptime
        df = df.withColumn("start_date_temp", date_format(col("starttime"),  "yyyy-MM-dd")) \
                .withColumn("start_time_temp", date_format(col("starttime"),  "HH:mm:00")) \
                .withColumn("stop_date_temp", date_format(col("stoptime"),  "yyyy-MM-dd")) \
                .withColumn("stop_time_temp", date_format(col("stoptime"),  "HH:mm:00"))
        
        # get bike_id from dim_bike
        df = df.join(df_bike, df.bike_nk == df_bike.bike_nk, "left") \
                .drop(df_bike.bike_nk)
        
        # get user_type_id from dim_user_type
        df = df.join(df_user_type, df.user_type_name == df_user_type.user_type_name, "left") \
                .drop(df_user_type.user_type_name)
        
        # get start_station_id from dim_station
        df = df.join(df_station, df.start_station_nk == df_station.station_nk, "left") \
                .drop(df_station.station_nk)
        # rename column station_id to start_station_id
        df = df.withColumnRenamed("station_id", "start_station_id")

        # get end_station_id from dim_station
        df = df.join(df_station, df.end_station_nk == df_station.station_nk, "left") \
                .drop(df_station.station_nk)
        # rename column station_id to end_station_id
        df = df.withColumnRenamed("station_id", "end_station_id")

        # get date_id from dim_date
        df = df.join(df_date, df.start_date_temp == df_date.date_actual, "left") \
                .drop(df_date.date_actual)
        # rename column date_id to start_date
        df = df.withColumnRenamed("date_id", "start_date")

        # get date_id from dim_date
        df = df.join(df_date, df.stop_date_temp == df_date.date_actual, "left") \
                .drop(df_date.date_actual)
        # rename column date_id to stop_date
        df = df.withColumnRenamed("date_id", "stop_date")

        # get time_id from dim_time
        df = df.join(df_time, df.start_time_temp == df_time.time_actual, "left") \
                .drop(df_time.time_actual)
        # rename column time_id to start_time
        df = df.withColumnRenamed("time_id", "start_time")

        # get time_id from dim_time
        df = df.join(df_time, df.stop_time_temp == df_time.time_actual, "left") \
                .drop(df_time.time_actual)
        # rename column time_id to stop_time
        df = df.withColumnRenamed("time_id", "stop_time")

        
        # drop unnecessary column created_at, postal code, birth year, start station name, end station name, 
        # start station latitude, start station longitude, end station latitude, end station longitude,
        # bike_nk, user_type_name, start_station_nk, end_station_nk
        if "postal code" in df.columns:
            df = df.drop("postal code")

        df = df.drop("created_at", "birth year", "start station name", "end station name", "gender",
                    "start station latitude", "start station longitude", "end station latitude", "end station longitude",
                    "bike_nk", "user_type_name", "start_station_nk", "end_station_nk", "starttime","stoptime", "station_nk",
                    "start_date_temp","stop_date_temp", "start_time_temp","stop_time_temp", "date_actual", "time_actual")
        # log message
        log_msg = spark.sparkContext\
            .parallelize([("warehouse", "transform", "success", "staging", "trip_data", current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
        
        return df
        
    except Exception as e:
        print(e)

        # log message
        log_msg = spark.sparkContext\
            .parallelize([("warehouse", "transform", "failed", "staging", "trip_data", current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
    finally:
        # load log
        load_log(spark, log_msg)