import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import platform

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

if platform.system() == "Windows":
    os.environ["HADOOP_HOME"] = "C:\\hadoop" 
    os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin" 
    os.environ["PYSPARK_PYTHON"] = "C:\\Python311\\python.exe" 
    os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Python311\\python.exe"


def get_spark_session(app_name: str, extra_packages: list = None) -> SparkSession:
    """
    returns a configured SparkSession with Delta Lake support.
    """
    builder = (SparkSession.builder
               .appName(app_name)
               .master("local[*]")
               .config("spark.sql.extensions", 
                       "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.driver.memory", "2g")
               .config("spark.driver.host", "127.0.0.1")
               .config("spark.driver.bindAddress", "127.0.0.1")
               )
    return configure_spark_with_delta_pip(builder, extra_packages = extra_packages or []
                                          ).getOrCreate()
