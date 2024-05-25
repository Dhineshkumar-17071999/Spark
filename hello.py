import pyspark
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

def print_hi(name):
    print(f"hi {name}")
    my_list = [1,2,3]
    spark = SparkSession.builder.appName("my first spark app").enableHiveSupport().getOrCreate()
    df = spark.createDataFrame(my_list, IntegerType())
    df.show()
    df.createOrReplaceTempView("temptable1")
    spark.sql("create table as select * from temptable1")


if __name__ == "__main__":
    print_hi("Dhinesh")