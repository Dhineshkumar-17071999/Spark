import logging
import logging.config

class Transform():
    logging.config.fileConfig("resources/configs/logging.conf")
    def __init__(self, spark):
        self.spark = spark

    def transform_data(self, df):
        logging.info("Transforming")
        df1 = df.na.drop()
        return df1