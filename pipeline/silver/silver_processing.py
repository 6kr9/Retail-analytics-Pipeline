import sys
import os
sys.path.append(os.environ.get("PROJECT_ROOT", "D:\\retail_pipeline"))

from pipeline.config import BRONZE_PATH, SILVER_PATH
from pipeline.spark_session import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from colorama import Fore, Style, init
init(autoreset=True)

#BRONZE_PATH = "D:\\retail_pipeline\\delta_tables\\bronze"
#SILVER_PATH = "D:\\retail_pipeline\\delta_tables\\silver"

def read_bronze(spark):
    """
    Reads the bronze Delta table and returns a DataFrame.
    """
    df = spark.read.format("delta").load(BRONZE_PATH)
    print(Fore.GREEN + f"Read {df.count()} records from bronze Delta table.")
    return df

def cast_types(df):
    """
    Casts the columns to appropriate data types.
    """
    print(Fore.GREEN + "Casting columns to appropriate data types.")
    df = (df
          .withColumn("quantity", F.col("quantity").cast(IntegerType()))
          .withColumn("invoicedate", F.col("invoicedate").cast(TimestampType()))
          .withColumn("price", F.col("price").cast(DoubleType()))
          )
    return df

def deduplicate(df):
    """
    Deduplicates the DataFrame based on all columns.
    """
    before_count = df.count()
    df = df.dropDuplicates()
    after_count = df.count()
    print(Fore.GREEN + f"Removed {before_count - after_count} duplicate records.")
    return df

def add_derived_columns(df):
    """
    Adds derived columns to the DataFrame.
    """
    print(Fore.GREEN + "Adding derived columns : is_cancelled, total_amount...")
    df = (df
          .withColumn("is_cancelled", F.col("invoice").startswith("C"))
          .withColumn("total_amount", F.col("quantity") * F.col("price"))
          )
    return df

def write_silver(df):
    """
    Writes the DataFrame to the silver Delta table.
    """
    print(Fore.GREEN + f"Writing {df.count()} records to silver delta table at {SILVER_PATH}.")
    (df.write
     .format("delta")
     .mode("overwrite")
     .save(SILVER_PATH)
     )

    print(Fore.GREEN + "Write to silver Delta table completed.")

def run():
    """
    Main function to run the silver processing pipeline.
    """
    spark = get_spark_session("Retailpipeline_silver")
    spark.sparkContext.setLogLevel("ERROR")

    df = read_bronze(spark)
    df = cast_types(df)
    df = deduplicate(df)
    df = add_derived_columns(df)
    print(Fore.GREEN + "Sample data:")  
    df.show(5, truncate=False)
    print(Fore.BLUE + "Schema:")
    df.printSchema()
    write_silver(df)

    silver_df = spark.read.format("delta").load(SILVER_PATH)
    print(Fore.GREEN + f"Read {silver_df.count()} records from silver Delta table.")

    spark.stop()

if __name__ == "__main__":
    run()        