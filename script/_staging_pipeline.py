import pyspark
from pyspark.sql import SparkSession
from src.staging.extract.extract_database import extract_database
from src.staging.extract.extract_csv import extract_csv
from src.staging.load.load_staging import load_staging
from pyspark.sql.functions import current_timestamp


def pipeline_stg():
    # create spark session
    spark = SparkSession \
        .builder \
        .appName("Pipeline Staging") \
        .getOrCreate()


    # Extract data from database
    df_user_type = extract_database(spark, 'user_type')
    df_station = extract_database(spark, 'station')
    df_bike = extract_database(spark, 'bike')



    # Extract data from csv
    df_trip_2020 = extract_csv(spark, 'bluebikes_tripdata_2020.csv')
    df_trip_2019 = extract_csv(spark, 'bluebikes_tripdata_2019.csv')


    # Load data to staging
    # add column created_at 
    df_user_type = df_user_type.withColumn("created_at", current_timestamp())
    df_station = df_station.withColumn("created_at", current_timestamp())
    df_bike = df_bike.withColumn("created_at", current_timestamp())
    df_trip_2019 = df_trip_2019.withColumn("created_at", current_timestamp())
    df_trip_2020 = df_trip_2020.withColumn("created_at", current_timestamp())


    load_staging(spark, df_user_type, "user_type", "db_bluebikes")
    load_staging(spark, df_station, "station", "db_bluebikes")
    load_staging(spark, df_bike, "bike", "db_bluebikes")
    load_staging(spark, df_trip_2019, "trip_data_2019", "csv")
    load_staging(spark, df_trip_2020, "trip_data_2020", "csv")

    spark.stop()