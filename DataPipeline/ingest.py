import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import logging
import logging.config
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from sqlalchemy import create_engine

class Ingestion():
    logging.config.fileConfig("resources/configs/logging.conf")
    def __init__(self,spark):
        self.spark = spark

    def ingest_data(self):
        logger = logging.getLogger("Ingest")
        logger.info("Ingesting started")
        logger.error("Ingesting started with error")
        # my_list = [1,2,3]
        # df = self.spark.createDataFrame(my_list, IntegerType())
        # df.show()
        # custome_df = self.spark.read.csv("retailstore.csv", header = True)
        custome_df = self.spark.sql("select * from fxxcoursedb.fx_course_table")
        # custome_df.show()
        # custome_df.describe().show()
        # custome_df.select('Country').show()
        # custome_df.groupBy("Country").count().show()
        # custome_df.filter("Salary > 3000").show()
        # custome_df.groupBy("gender").agg({"salary":"avg","age":"max"}).show()
        # custome_df.orderBy("Salary").show()
        return custome_df
    
    def read_from_pg(self):
        engine = create_engine('postgresql://postgres:1234@localhost/postgres')
        sql_query = "select * from public.count_records_1"
        pdDF = pd.read_sql_query(sql_query, engine)
        for column in pdDF.columns:
            pdDF[column] = pdDF[column].astype(str)
        
        schema = StructType([
            StructField("family_count",StringType(),True),
            StructField("family_member_count",StringType(),True),
            StructField("screening_count",StringType(),True),
            StructField("male_count",StringType(),True),
            StructField("female_count",StringType(),True),
            StructField("transgender_count",StringType(),True),
            StructField("member_18_45_count",StringType(),True),
            StructField("member_45_above",StringType(),True),
            StructField("verified_mem_count",StringType(),True),
            StructField("mem_married_male",StringType(),True),
            StructField("mem_married_female",StringType(),True),
            StructField("mem_0_1",StringType(),True),
            StructField("mem_2_5",StringType(),True),
            StructField("mem_6_12",StringType(),True),
            StructField("mtm_count",StringType(),True),
            StructField("drug_issued_count",StringType(),True),
            StructField("mem_46_59",StringType(),True),
            StructField("mem_60_above",StringType(),True),
            StructField("mem_below_3",StringType(),True),
            StructField("mem_3_6",StringType(),True),
            StructField("mem_7_9",StringType(),True),
            StructField("mem_10_19",StringType(),True),
            StructField("resident_count",StringType(),True),
            StructField("death_count",StringType(),True),
            StructField("migrants_count",StringType(),True),
            StructField("non_traceables_count",StringType(),True),
            StructField("dublicate_count",StringType(),True),
            StructField("member_added",StringType(),True),
            StructField("family_added",StringType(),True),
            StructField("urban_scr",StringType(),True),
            StructField("rural_scr",StringType(),True),
            StructField("municipalty_scr",StringType(),True),
            StructField("corporation_scr",StringType(),True),
            StructField("uhc_total_op",StringType(),True),
            StructField("uhc_male_op",StringType(),True),
            StructField("uhc_female_op",StringType(),True),
            StructField("uhc_referral",StringType(),True),
            StructField("uhc_op_facility_daya",StringType(),True),
            StructField("uhc_labtest_day",StringType(),True),
            StructField("uhc_drug_day",StringType(),True)
        ])
        sparkDF = self.spark.createDataFrame(pdDF, schema=schema)
        sparkDF.show()

    def read_from_pg_using_jdbc(self):
        jdbcDF = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres") \
            .option("dbtable", "public.count_records_1") \
            .option("user", "postgres") \
            .option("password", "1234") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        jdbcDF.show()