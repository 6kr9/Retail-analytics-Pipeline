import sys
import os
from unittest import result

sys.path.append(os.environ.get("PROJECT_ROOT", "D:\\retail_pipeline"))

from pipeline.config import SILVER_PATH, ANOMALY_PATH, GOLD_DAILY_PATH, GOLD_MONTHLY_PATH, GOLD_TOP_ANOMALIES_PATH
from pipeline.spark_session import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from colorama import Fore, Style, init
init(autoreset=True)

#SILVER_PATH = "D:\\retail_pipeline\\delta_tables\\silver"
#ANOMALY_PATH = "D:\\retail_pipeline\\delta_tables\\anomaly_results"

#GOLD_DAILY_PATH = "D:\\retail_pipeline\\delta_tables\\gold_daily_product_sales"
#GOLD_MONTHLY_PATH = "D:\\retail_pipeline\\delta_tables\\gold_monthly_country_sales"
#GOLD_TOP_ANOMALIES_PATH = "D:\\retail_pipeline\\delta_tables\\gold_top_anomalies"


def read_sources(spark):
    """
    Reads the silver Delta table and anomaly results Delta table and returns DataFrames.
    """
    silver_df = spark.read.format("delta").load(SILVER_PATH)
    anomaly_df = spark.read.format("delta").load(ANOMALY_PATH)

    print(Fore.GREEN + f"Read {silver_df.count()} records from silver Delta table.")
    print(Fore.GREEN + f"Read {anomaly_df.count()} records from anomaly results Delta table.")

    return silver_df, anomaly_df

def get_description_lookup(silver_df):
    """
    one desciption per stockcode, picks the most frequently used description,
    since the same stockcode can have minor text variations across rows.
    """

    print(Fore.GREEN + "Building stockcode -> description lookup...")
    desc_counts = (silver_df.groupBy("stockcode", "description")
                   .agg(F.count("*").alias("cnt")))

    window = Window.partitionBy("stockcode").orderBy(F.col("cnt").desc())

    lookup = (desc_counts.withColumn("rank", F.row_number().over(window))
              .filter(F.col("rank") == 1)
              .select("stockcode", "description"))

    return lookup

def get_top_country_lookup(silver_df):
    """
    top country per (sale_date, stockcode) by reveune - used to attach
    descriptive context to the date-product grain without changing 
    what was actually scored."""

    print(Fore.GREEN + "Building date-product -> top country lookup...")
    df = silver_df.withColumn("sale_date", F.to_date("invoicedate"))
    non_cancelled = df.filter(~F.col("is_cancelled"))

    country_rev = (non_cancelled.groupBy("sale_date", "stockcode", "country")
                   .agg(F.sum("total_amount").alias("country_revenue")))

    window = Window.partitionBy("sale_date", "stockcode").orderBy(F.col("country_revenue").desc())

    top_country = (country_rev.withColumn("rank", F.row_number().over(window))
                   .filter(F.col("rank") == 1)
                   .select("sale_date", "stockcode",
                           F.col("country").alias("top_country")))

    return top_country

def build_gold_daily_product_sales(anomaly_df, desc_lookup, top_country_lookup):

    print(Fore.GREEN + "Building gold daily product sales table...")

    result = (anomaly_df.join(desc_lookup, on = "stockcode", how = "left")
              .join(top_country_lookup, on = ["sale_date", "stockcode"], how  = "left"))

    print(f"gold daily product sales row count: {result.count()}")

    return result

def build_gold_monthly_country_sales(silver_df):
    print(Fore.GREEN + "Building gold monthly country sales...")

    df = silver_df.withColumn("sale_month", F.date_format("invoicedate", "yyyy-MM"))
    non_cancelled = df.filter(~F.col("is_cancelled"))

    result = (non_cancelled.groupBy("sale_month", "country")
              .agg(
                  F.sum("total_amount").alias("total_revenue"),
                  F.sum("quantity").alias("total_quantity"),
                  F.count("*").alias("total_tranasactions"),
                  F.countDistinct("stockcode").alias("unique_products_sold")
              ))

    print(f"gold_monthly_country_sales row count: {result.count()}")
    return result

def build_gold_top_anomalies(gold_daily_df, top_n = 100):

    print(Fore.GREEN + f"Building gold_top_anomalies (top {top_n})...")
    result = (gold_daily_df
              .filter(F.col("is_anomaly"))
              .orderBy(F.col("anomaly_score").asc())
              .limit(top_n))

    print(f"gold_top_anomalies row count: {result.count()}")
    return result

def write_delta(df, path, label):
     print(Fore.GREEN + f"Writing {label} to {path}...")
     (df.write.format("delta")
      .mode("overwrite")
      .save(path))

     print(Fore.GREEN + f"Successfully wrote {label} to {path}") 

def run():
    spark = get_spark_session("RetailPipeline_Gold")

    silver_df, anomaly_df = read_sources(spark)

    desc_lookup = get_description_lookup(silver_df)
    top_country_lookup = get_top_country_lookup(silver_df)

    gold_daily_df = build_gold_daily_product_sales(anomaly_df, desc_lookup, top_country_lookup)

    print(Fore.GREEN + f"Sample gold daily product sales:")
    gold_daily_df.show(5, truncate = False)
    write_delta(gold_daily_df, GOLD_DAILY_PATH, "gold_daily_product_sales")

    gold_monthly_df = build_gold_monthly_country_sales(silver_df)
    print(Fore.GREEN + f"Sample gold monthly country sales:")
    gold_monthly_df.show(5, truncate = False)     
    write_delta(gold_monthly_df, GOLD_MONTHLY_PATH, "gold_monthly_country_sales")

    gold_top_anomalies_df = build_gold_top_anomalies(gold_daily_df, top_n = 100)
    print(Fore.GREEN + f"Sample gold top anomalies:")
    gold_top_anomalies_df.show(5, truncate = False)
    write_delta(gold_top_anomalies_df, GOLD_TOP_ANOMALIES_PATH, "gold_top_anomalies")

    print(Fore.GREEN + "Gold Layer pipeline completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run()    
