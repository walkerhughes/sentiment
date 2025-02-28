from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.mongo.hooks.mongo import MongoHook
import json

class CustomGCSToMongoDBOperator(BaseOperator):
    """
    Moves data from Google Cloud Storage to MongoDB.
    """
    
    def __init__(
        self,
        gcs_bucket,
        gcs_object,
        mongo_db,
        mongo_collection,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.gcs_bucket = gcs_bucket
        self.gcs_object = gcs_object
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        
    def execute(self, context):
        # Download from GCS
        gcs_hook = GCSHook()
        data_string = gcs_hook.download(
            bucket_name=self.gcs_bucket,
            object_name=self.gcs_object
        )
        data = json.loads(data_string)
        
        # Upload to MongoDB
        mongo_hook = MongoHook(conn_id='mongo_default')
        mongo_hook.insert_many(
            mongo_collection=self.mongo_collection,
            mongo_db=self.mongo_db,
            docs=data
        )
        
        return f"Transferred {len(data)} documents to MongoDB" 