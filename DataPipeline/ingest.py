import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

class Ingestion():
    def __init__(self,spark):
        self.spark = spark

    def ingest_data(self):
        print("Ingesting")
        my_list = [1,2,3]
        df = self.spark.createDataFrame(my_list, IntegerType())
        df.show()