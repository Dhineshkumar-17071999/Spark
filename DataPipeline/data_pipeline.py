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
            .appName("my first spark app")\
                .enableHiveSupport().getOrCreate()

    def run_pipeline(self):
        logging.info("run pipeline started")
        ingest_process = ingest.Ingestion(self.spark)
        df = ingest_process.ingest_data()
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
    logging.info('run pipeline created')
    pipeline.run_pipeline()