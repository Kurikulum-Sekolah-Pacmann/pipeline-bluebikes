import pyspark
from pyspark.sql import SparkSession

from src.warehouse.extract.extract_staging import extract_staging, extract_staging_filter
from src.warehouse.load.load_warehouse import load_warehouse
from src.warehouse.transformation.user_type import transform_user_type
from src.warehouse.transformation.bike import transform_bike
from src.warehouse.transformation.station import transform_station
from src.warehouse.transformation.fact_trip import transform_fact_trip



# # create spark session
spark = SparkSession \
    .builder \
    .appName("Pipeline Warehouse") \
    .getOrCreate()


df_trip = extract_staging_filter(spark, 'combined_trip_data',"usertype", "Subscriber")

trip_transformed = transform_fact_trip(spark, df_trip)

load_warehouse(spark, trip_transformed, "fact_trip_data", 'staging')
