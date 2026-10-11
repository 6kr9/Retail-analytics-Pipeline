import sys
import os
sys.path.append("D:\\retail_pipeline")

from pipeline.spark_session import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from delta.tables import DeltaTable
from colorama import Fore, Style, init
init(autoreset=True)

KAFKA_TOPIC = "retail-transactions"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

STREAMING_BRONZE_PATH = "D:\\retail_pipeline\\delta_tables\\streaming_bronze"
CHECKPOINT_PATH = "D:\\retail_pipeline\\pipeline\\streaming\\checkpoints\\streaming_bronze_checkpoint"

message_schema = StructType([
    StructField("invoice", StringType(), True),
    StructField("stockcode", StringType(), True),
    StructField("description", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("invoicedate", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("customer_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("is_cancelled", BooleanType(), True),
    StructField("total_amount", DoubleType(), True)
    ])

def read_kafka_stream(spark):

    print(Fore.GREEN + f"Reading from Kafka topic {KAFKA_TOPIC}...")
    raw_stream = (spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                  .option("subscribe", KAFKA_TOPIC)
                  .option("startingOffsets", "earliest")
                  .load())

    return raw_stream

def parse_messages(raw_stream):
    print(Fore.GREEN + "Parsing messages...")
    parsed = (raw_stream
              .selectExpr("CAST(value as STRING) as json_str")
              .select(F.from_json("json_str", message_schema).alias("data"))
              .select("data.*")
              .withColumn("timestamp", F.col("invoicedate").cast(TimestampType()))
                      )

    return parsed

def upsert_to_bronze(batch_df, batch_id):

    """ called once per micro-batch . uses merge so re-processing the same message 
    overwrites the existing record instead of creating duplicates. """

    print(Fore.GREEN + f"Upserting batch {batch_id} to Bronze Delta table..."
          f"({batch_df.count()} records)")

    if batch_df.rdd.isEmpty():
        print(Fore.YELLOW + f"Batch {batch_id} is empty. Skipping upsert.")
        return

    spark = batch_df.sparkSession

    if not DeltaTable.isDeltaTable(spark, STREAMING_BRONZE_PATH):

        print(Fore.GREEN + "Creating streaming_bronze Delta table for the first time...")

        (batch_df.write
         .format("delta")
         .mode("overwrite")
         .save(STREAMING_BRONZE_PATH))

        return

    delta_table = DeltaTable.forPath(spark, STREAMING_BRONZE_PATH)

    delta_table.alias("target")\
    .merge(
            batch_df.alias("source"),
            "target.invoice = source.invoice AND target.stockcode = source.stockcode"
        )\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

    print(Fore.GREEN + f"Batch {batch_id} upserted successfully to Bronze Delta table.")

def run():

    spark = get_spark_session("RetailPipeline_Consumer",
                              extra_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"])
    spark.sparkContext.setLogLevel("ERROR")

    raw_stream = read_kafka_stream(spark)
    parsed_stream = parse_messages(raw_stream)

    query = (parsed_stream.writeStream
             .foreachBatch(upsert_to_bronze)
             .option("checkpointLocation", CHECKPOINT_PATH)
             .trigger(processingTime="10 seconds")
             .start())

    print(Fore.GREEN + "Streaming consumer started. "
    "Waiting for Messages... Press Ctrl+C to stop.")  
    query.awaitTermination()

if __name__ == "__main__":
   run()      
    





