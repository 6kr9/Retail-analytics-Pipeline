import sys
import os

sys.path.append(os.environ.get("PROJECT_ROOT", "D:\\retail_pipeline"))

from pipeline.config import SILVER_PATH, ANOMALY_PATH
from pipeline.spark_session import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from colorama import Fore, Style, init
init(autoreset=True)

#SILVER_PATH = "D:\\retail_pipeline\\delta_tables\\silver"
#ANOMALY_PATH = "D:\\retail_pipeline\\delta_tables\\anomaly_results"

def read_silver(spark):
    """
    Reads the silver Delta table and returns a DataFrame.
    """
    df = spark.read.format("delta").load(SILVER_PATH)
    print(Fore.GREEN + f"Read {df.count()} records from silver Delta table.")
    return df

def aggregate_daily_product_sales(df):
    """
    Aggregates daily product sales.
    """
    print(Fore.GREEN + "Aggregating daily product sales.")

    df = df.withColumn("sale_date", F.to_date("invoicedate"))

    non_cancelled = df.filter(~F.col("is_cancelled"))   
    cancelled = df.filter(F.col("is_cancelled"))

    sales_agg = (non_cancelled.groupBy("sale_date", "stockcode")
                 .agg(
                     F.sum("quantity").alias("total_quantity"),
                     F.sum("total_amount").alias("total_revenue"),
                     F.count("*").alias("total_transactions"),
                     F.avg("price").alias("avg_price")
                 ))

    cancel_agg = (cancelled.groupBy("sale_date", "stockcode")
                  .agg(
                      F.count("*").alias("num_cancellations"))
                  )
    result = sales_agg.join(cancel_agg, ["sale_date", "stockcode"], "left").fillna({"num_cancellations": 0})

    print(f"Aggregated to {result.count()} daily product sales records.")
    return result

def detect_anomalies(df):
    """
    Detects anomalies in daily product sales.
    """
    print(Fore.GREEN + "Detecting anomalies in daily product sales.")

    from sklearn.ensemble import IsolationForest

    print(Fore.GREEN + "Converting aggregated spark dataframe to pandas for scoring...")
    pdf  = df.toPandas()

    feature_cols = ["total_quantity", "total_revenue", "total_transactions", "avg_price", "num_cancellations"]

    print("Training Isolation Forest model...")

    model = IsolationForest(
        n_estimators=100,
        contamination=0.05, 
        random_state=42
        )

    model.fit(pdf[feature_cols])

    pdf["anomaly_score"] = model.decision_function(pdf[feature_cols])
    pdf["is_anomaly"] = model.predict(pdf[feature_cols]) == -1

    num_anomalies = pdf["is_anomaly"].sum()
    print(Fore.RED + f"Flagged {num_anomalies} anomalous date-product groups"
          f"out of {len(pdf)} ({num_anomalies / len(pdf) * 100:.2f}%)")

    return pdf

def write_anomaly_results(spark, pdf):

    print(f"Writing anomaly result to {ANOMALY_PATH}...")
    spark_df = spark.createDataFrame(pdf)
    (spark_df.write 
     .format("delta")
     .mode("overwrite")
     .save(ANOMALY_PATH)
    )

    print(Fore.GREEN + "Write to anomaly results Delta table completed.")

def run():
    """
    Main function to run the anomaly detection pipeline.
    """
    spark = get_spark_session(
        "Retailpipeline_anomaly_detection"
    )

    silver_df = read_silver(spark)
    aggregated_df = aggregate_daily_product_sales(silver_df)
    print(Fore.GREEN + "Sample aggregated results:")
    aggregated_df.show(5, truncate=False)
    anomaly_pdf = detect_anomalies(aggregated_df)
    print(Fore.GREEN + "Sample Anomaly results:")
    print(anomaly_pdf[anomaly_pdf["is_anomaly"]].head(10))
    write_anomaly_results(spark, anomaly_pdf)
    result_df = spark.read.format("delta").load(ANOMALY_PATH)
    print(Fore.GREEN + f"Read {result_df.count()} records from anomaly results Delta table.")

    print(Fore.GREEN + "Anomaly detection pipeline completed successfully.")   

    print(anomaly_pdf[anomaly_pdf["is_anomaly"]].sort_values("anomaly_score").head(10))

    spark.stop()

if __name__ == "__main__":
    run()     