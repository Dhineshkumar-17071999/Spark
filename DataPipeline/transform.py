import logging
import logging.config

class Transform():
    logging.config.fileConfig("resources/configs/logging.conf")
    def __init__(self, spark):
        self.spark = spark

    def transform_data(self, df):
        logger = logging.getLogger("Transform")
        logger.info("Transforming")
        # df1 = df.na.drop()
        df1 = df.na.fill('Unknown',["family_count"])
        df2 = df1.na.fill("0",["family_member_count"])
        return df2