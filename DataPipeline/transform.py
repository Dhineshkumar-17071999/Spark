class Transform():
    def __init__(self, spark):
        self.spark = spark

    def transform_data(self, df):
        print("Transforming")
        df1 = df.na.drop()
        return df1