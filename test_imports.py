# Create a test file: test_imports.py

# Try importing the problematic module
import googleapiclient.discovery

print("Google API Client imported successfully!")

# Test other imports
from google.cloud import storage
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.transfers.gcs_to_mongodb import GCSToMongoDBOperator

print("All imports successful!")