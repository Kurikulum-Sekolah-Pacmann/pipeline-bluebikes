from src.utils.helper import bluebikes_engine
from pyspark.sql import SparkSession

PATH = "data/"
def extract_database(spark: SparkSession, table_name):
    # get config
    DB_URL, DB_USER, DB_PASS = bluebikes_engine()

    # set config
    connection_properties = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver" # set driver postgres
    }

    df = spark \
              .read \
              .jdbc(url = DB_URL,
                    table = table_name,
                    properties = connection_properties)
    
    return df


def extract_csv(spark: SparkSession, file_name):

    df = spark.read.csv(PATH + file_name, header=True)

    return df