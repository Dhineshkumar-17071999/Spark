import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

import ingest
import transform
import persist
import logging
import logging.config

class Pipeline:
    logging.config.fileConfig("resources/configs/logging.conf")

    def create_spark_session(self):
        self.spark = SparkSession.builder\
            .config("spark.jars","postgresql-42.6.2.jar")\
            .appName("my first spark app")\
                .enableHiveSupport().getOrCreate()
    
    def create_hive_table(self):
        # self.spark.sql("create database if not exists fxxcoursedb")
        # self.spark.sql("create table if not exists fxxcoursedb.fx_course_table (course_id string, course_name string, author_name string, no_of_reviews string)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (1,'JAVA','FutureX','45')")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (2,'Java','FutureXSkill',56)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (3,'Big Data','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (4,'Linux','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (5,'Microservices','Future',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (6,'CMS','',100)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (7,'Python','FutureX','')")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (8,'CMS','Future',56)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (9,'Dot Net','FutureXSkill',34)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (10,'Ansible','FutureX',123)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (11,'Jenkins','Future',32)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (12,'Chef','FutureX',121)")
        self.spark.sql("insert into fxxcoursedb.fx_course_table VALUES (13,'Go Lang','',105)")
        #Treat empty string as null
        self.spark.sql("alter table fxxcoursedb.fx_course_table set tblproperties('serialization.null.format'='')")
        
    def run_pipeline(self):
        logging.info("run pipeline started")
        ingest_process = ingest.Ingestion(self.spark)
        # ingest_process.read_from_pg()
        df = ingest_process.read_from_pg_using_jdbc()
        # df = ingest_process.ingest_data()
        df.show()
        transform_process = transform.Transform(self.spark)
        transformed_df = transform_process.transform_data(df)
        transformed_df.show()
        persist_process = persist.Persist(self.spark)
        persist_process.persist_data(transformed_df)
        logging.info("run pipeline ended")
        return
        
if __name__ == "__main__":
    logging.info('application started with info')
    logging.warning('application started with warning')
    logging.debug('application started with debug')
    logging.error('application started with error')
    
    logging.info('spark application started')
    pipeline = Pipeline()
    logging.info('spark session created')
    pipeline.create_spark_session()
    # pipeline.create_hive_table()
    logging.info('run pipeline created')
    pipeline.run_pipeline()