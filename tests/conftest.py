import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
     spark = SparkSession.builder \
         .appName("UnitTests") \
         .master("local[1]") \
         .config("spark.sql.shuffle.partitions", "1") \
         .getOrCreate()
     yield spark
     spark.stop()