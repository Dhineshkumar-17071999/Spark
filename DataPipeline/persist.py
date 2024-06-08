import logging
import logging.config

class Persist():
    logging.config.fileConfig("resources/configs/logging.conf")
    def __init__(self, spark):
        self.spark = spark

    def persist_data(self,df):
        try:
            logger = logging.getLogger("Persist")
            logger.info("Persisting")
            logger.warning("Persisting with warning")
            user = "postgres"
            password = "1234"
            # df.coalesce(1).write.option("header","true").csv("transformed_retailstore")
            df.write\
            .mode("append")\
            .format("jdbc")\
            .option("url","jdbc:postgresql://localhost:5432/postgres")\
            .option("dbtable","public.count_records_tes")\
            .option("user",user)\
            .option("password",password)\
            .option("driver", "org.postgresql.Driver") \
            .save()
        except Exception as e:
            logger.error("An error occured while persisting data :"+str(e))
            raise Exception("HDFS directory already exists")
