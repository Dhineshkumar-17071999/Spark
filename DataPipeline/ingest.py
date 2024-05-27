import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

class Ingestion():
    def __init__(self,spark):
        self.spark = spark

    def ingest_data(self):
        print("Ingesting")
        # my_list = [1,2,3]
        # df = self.spark.createDataFrame(my_list, IntegerType())
        # df.show()
        custome_df = self.spark.read.csv("retailstore.csv", header = True)
        custome_df.show()
        custome_df.describe().show()
        custome_df.select('Country').show()
        custome_df.groupBy("Country").count().show()
        custome_df.filter("Salary > 3000").show()
        custome_df.groupBy("gender").agg({"salary":"avg","age":"max"}).show()
        custome_df.orderBy("Salary").show()