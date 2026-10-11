import sys
import os
import json
import time
from datetime import datetime

sys.path.append("D:\\retail_pipeline")

from pipeline.spark_session import get_spark_session
from pyspark.sql import functions as F
from kafka import KafkaProducer
from colorama import Fore, Style, init
init(autoreset=True)

SILVER_PATH = "D:\\retail_pipeline\\delta_tables\\silver"
KAFKA_TOPIC = "retail-transactions"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

REPLAY_MONTH = "2011-11"
COMPRESSION_FACTOR = 3000
MAX_DELAY_SECONDS = 2.0

def load_month_sample(spark):

    print(Fore.GREEN + f"Reading Silver and filtering to month {REPLAY_MONTH}...")
    df = spark.read.format("delta").load(SILVER_PATH)
    df = df.withColumn("sale_month", F.date_format("invoicedate" , "yyyy-MM"))
    df = df.filter(F.col("sale_month") == REPLAY_MONTH)
    df = df.orderBy("invoicedate")
    count = df.count()
    print(Fore.GREEN + f"Loaded {count} rows for {REPLAY_MONTH}.")
    pdf = df.drop("sale_month").toPandas()
    return pdf

def row_to_json(row):

    record = row.to_dict()

    for key,val in record.items():
        if isinstance(val, (datetime,)):
            record[key] = val.isoformat()
        elif hasattr(val, "item"):
            record[key] = val.item()

    return record

def compute_delays(pdf):

    timestamps = pdf["invoicedate"]
    gaps = timestamps.diff().dt.total_seconds().fillna(0)
    compressed = (gaps / COMPRESSION_FACTOR).clip(upper=MAX_DELAY_SECONDS)
    compressed.iloc[0] = 0
    return compressed

def run():

    spark = get_spark_session("RetailPipeline_Producer")
    spark.sparkContext.setLogLevel("ERROR")

    pdf = load_month_sample(spark)
    spark.stop()

    if len(pdf) == 0:
        print(Fore.RED + f"No rows found for month {REPLAY_MONTH}. Aborting.")
        return

    delays = compute_delays(pdf)

    print(Fore.GREEN + f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    producer = KafkaProducer(
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        value_serializer = lambda v:json.dumps(v).encode("utf-8"),
        key_serializer = lambda k:k.encode("utf-8")if k else None
    )

    print(Fore.GREEN + f"Replaying {len(pdf)} transactions to Kafka topic {KAFKA_TOPIC}...")
    sent = 0

    for idx,row in pdf.iterrows():
        delay = delays.loc[idx]
        if delay > 0:
            time.sleep(delay)

        record = row_to_json(row)
        key = str(record.get("stockcode",""))

        producer.send(KAFKA_TOPIC, key = key, value = record)
        sent += 1

        if sent % 500 == 0:
            print(Fore.GREEN + f"Sent {sent}/{len(pdf)} messages...") 

    producer.flush()
    producer.close()
    print(Fore.GREEN + f"Finished sending {sent} messages to Kafka topic {KAFKA_TOPIC}.")


if __name__ == "__main__":
    run()               


