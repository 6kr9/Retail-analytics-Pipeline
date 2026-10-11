import sys
import os
sys.path.append(os.environ.get("PROJECT_ROOT", "D:\\retail_pipeline"))

from pipeline.config import RAW_DATA_PATH, BRONZE_PATH
from pipeline.spark_session import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from colorama import Fore, Style, init
init(autoreset=True)

#RAW_DATA_PATH = "D:\\retail_pipeline\\data\\online_retail_II.csv"
#BRONZE_PATH = "D:\\retail_pipeline\\delta_tables\\bronze"

def read_raw_data(spark):
    """
    Reads the raw data from the CSV file and returns a DataFrame.
    """
    #schema = StructType([
    #    StructField("Invoice", StringType(), True),
    #    StructField("StockCode", StringType(), True),
    #    StructField("Description", StringType(), True),
    #    StructField("Quantity", IntegerType(), True),
    #    StructField("InvoiceDate", TimestampType(), True),
    #    StructField("Price", DoubleType(), True),
    #    StructField("CustomerID", IntegerType(), True),
   #     StructField("Country", StringType(), True)
   # ])
    
    df = spark.read.csv(RAW_DATA_PATH, header=True, inferSchema=True)#schema=schema)
    print(Fore.GREEN + f"Read {df.count()} records from raw data.")
    return df

def clean_column_names(df):
    """
    Cleans the column names by converting them to lowercase and replacing spaces with underscores.
    """

    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower().replace(" ","_"))
    return df

def write_bronze(df):
    """
    Writes the DataFrame to the bronze Delta table.
    """
    print(Fore.GREEN + f"Writing {df.count()} records to bronze Delta table at {BRONZE_PATH}.")
    df.write.format("delta").mode("overwrite").save(BRONZE_PATH)
    print(Fore.GREEN + "Write to bronze Delta table completed.")

def run():
    """
    Main function to run the bronze ingestion pipeline.
    """
    spark = get_spark_session("RetailPipeline_bronze")
    spark.sparkContext.setLogLevel("ERROR")
    df = read_raw_data(spark)
    df = clean_column_names(df)
    print(Fore.GREEN + "Sample data:")
    df.show(5, truncate=False)
    print(Fore.BLUE + "Schema:")
    df.printSchema()
    write_bronze(df)

    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    print(Fore.GREEN + f"Read {bronze_df.count()} records from bronze Delta table.")
    #input(Fore.YELLOW + "Press Enter to exit...")
    spark.stop()

if __name__ == "__main__":
    run()    