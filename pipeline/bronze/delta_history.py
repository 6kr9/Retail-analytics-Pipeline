import sys
import os
sys.path.append("D:\\retail_pipeline")

from pipeline.spark_session import get_spark_session
from pipeline.config import BRONZE_PATH, RAW_DATA_PATH
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from colorama import Fore, Style, init
init(autoreset=True)

#RAW_DATA_PATH = "D:\\retail_pipeline\\data\\online_retail_II.csv"
#BRONZE_PATH = "D:\\retail_pipeline\\delta_tables\\bronze"

def run():
    """
    Main function to run the bronze ingestion pipeline.
    """
    spark = get_spark_session("RetailPipeline_bronze")
    spark.sparkContext.setLogLevel("ERROR")
    delta_table = DeltaTable.forPath(spark, BRONZE_PATH)
    delta_table.history(10).select(
        "version",
        "timestamp",
        "operation",
        "operationParameters"
    ).show(truncate=False)

if __name__== "__main__":
    run()