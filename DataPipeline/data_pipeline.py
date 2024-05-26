import ingest
import transform
import persist

class Pipeline:
    def run_pipeline(self):
        print("running pipeline")
        ingest_process = ingest.Ingestion()
        ingest_process.ingest_data()
        transform_process = transform.Transform()
        transform_process.transform_data()
        persist_process = persist.Persist()
        persist_process.persist_data()
        
if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.run_pipeline()