import logging
import logging.config

class Persist():
    logging.config.fileConfig("resources/configs/logging.conf")
    def __init__(self, spark):
        self.spark = spark

    def persist_data(self,df):
        logging.info("Persisting")
        logging.warning("Persisting with warning")
        df.coalesce(1).write.option("header","true").csv("transformed_retailstore")
