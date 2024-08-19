# Function Extarct with log
from src.utils.helper import wh_engine
from pyspark.sql import SparkSession

def extract_warehouse(spark: SparkSession, table_name: str, columns_to_select: list):
    # get config
    DB_URL, DB_USER, DB_PASS = wh_engine()

    # set config
    connection_properties = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver" # set driver postgres
    }
    
    try:
        # read data specific column
        df = spark \
                .read \
                .jdbc(url = DB_URL,
                        table = table_name,
                        properties = connection_properties)
        
        # Select specific columns
        df = df.select(*columns_to_select)
        return df
    except Exception as e:
        print(e)