import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

import ingest
import transform
import persist

class Pipeline:
    
    def create_spark_session(self):
        self.spark = SparkSession.builder\
            .appName("my first spark app")\
                .enableHiveSupport().getOrCreate()

    def run_pipeline(self):
        print("running pipeline")
        ingest_process = ingest.Ingestion(self.spark)
        df = ingest_process.ingest_data()
        df.show()
        transform_process = transform.Transform(self.spark)
        transformed_df = transform_process.transform_data(df)
        transformed_df.show()
        persist_process = persist.Persist(self.spark)
        persist_process.persist_data(transformed_df)
        return
        
if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()