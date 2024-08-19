import pyspark
from pyspark.sql import SparkSession

from src.warehouse.extract.extract_staging import extract_staging
from src.warehouse.load.load_warehouse import load_warehouse
from src.warehouse.transformation.user_type import transform_user_type
from src.warehouse.transformation.bike import transform_bike
from src.warehouse.transformation.station import transform_station
from src.warehouse.transformation.fact_trip import transform_fact_trip
from src.warehouse.transformation.fact_bike_usage import transform_fact_bike_usage


def pipeline_wh():
    # # create spark session
    spark = SparkSession \
        .builder \
        .appName("Pipeline Warehouse") \
        .getOrCreate()


    # Extarc Data fro Dmension Table
    df_user_type = extract_staging(spark, 'user_type')
    df_station = extract_staging(spark, 'station')
    df_bike = extract_staging(spark, 'bike')

    user_type_transformed = transform_user_type(spark, df_user_type)
    load_warehouse(spark, user_type_transformed, "dim_user_type", 'staging')

    bike_transformed = transform_bike(spark, df_bike)
    load_warehouse(spark, bike_transformed, "dim_bike", 'staging')

    station_transformed = transform_station(spark, df_station)
    load_warehouse(spark, station_transformed, "dim_station", 'staging')



    df_trip = extract_staging(spark, 'combined_trip_data')

    trip_transformed = transform_fact_trip(spark, df_trip)
    load_warehouse(spark, trip_transformed, "fact_trip_data", 'staging')

    fact_bike_usage = transform_fact_bike_usage(spark)
    load_warehouse(spark, fact_bike_usage, "fact_bike_usage", 'staging')
