import os

PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "D:\\retail_pipeline")

BRONZE_PATH = os.path.join(PROJECT_ROOT, "delta_tables", "bronze")
SILVER_PATH = os.path.join(PROJECT_ROOT, "delta_tables", "silver")
ANOMALY_PATH = os.path.join(PROJECT_ROOT, "delta_tables", "anomaly_results")
GOLD_DAILY_PATH = os.path.join(PROJECT_ROOT, "delta_tables", "gold_daily_product_sales")
GOLD_MONTHLY_PATH = os.path.join(PROJECT_ROOT, "delta_tables", "gold_monthly_country_sales")
GOLD_TOP_ANOMALIES_PATH = os.path.join(PROJECT_ROOT, "delta_tables", "gold_top_anomalies")

STREAMING_BRONZE_PATH = os.path.join(PROJECT_ROOT, "delta_tables", "streaming_bronze")
CHECKPOINT_PATH = os.path.join(PROJECT_ROOT, "pipeline", "streaming", "checkpoints", "streaming_bronze_checkpoint")

RAW_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "online_retail_II.csv")
