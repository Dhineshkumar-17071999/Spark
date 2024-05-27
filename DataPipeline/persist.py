class Persist():
    def __init__(self, spark):
        self.spark = spark

    def persist_data(self,df):
        print("Persisting")
        df.coalesce(1).write.option("header","true").csv("transformed_retailstore")
