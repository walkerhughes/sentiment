import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.transfers.gcs_to_mongodb import GCSToMongoDBOperator
from google.cloud import storage
import requests
import json
import googleapiclient.discovery
from pymongo import MongoClient

# Environment variables
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET = os.getenv('GCS_BUCKET')
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Step 1: Get Alphavantage Data
def fetch_alphavantage_data(**context):
    tickers = ['AAPL', 'NVDA', 'MSFT', 'AMZN', 'GOOG', 'META', 'TSLA']
    size = 1000
    max_date_time = '20220101T0001'
    all_results = []
    timestamp = datetime.utcnow().isoformat()
    
    for ticker in tickers:
        url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&time_from={max_date_time}&limit={size}&apikey={ALPHA_VANTAGE_API_KEY}'
        data = requests.get(url).json()
        
        if 'feed' in data:
            for article in data['feed']:
                article_doc = {
                    'ticker': ticker,
                    'title': article.get('title'),
                    'url': article.get('url'),
                    'time_published': article.get('time_published'),
                    'authors': article.get('authors', []),
                    'summary': article.get('summary'),
                    'source': article.get('source'),
                    'overall_sentiment_score': article.get('overall_sentiment_score'),
                    'overall_sentiment_label': article.get('overall_sentiment_label'),
                    'ticker_sentiment': article.get('ticker_sentiment', []),
                    'topics': article.get('topics', []),
                    'collection_timestamp': timestamp
                }
                all_results.append(article_doc)
    
    return all_results

# Step 2: Get YouTube Data
def fetch_youtube_data(**context):
    alphavantage_data = context['task_instance'].xcom_pull(task_ids='fetch_alphavantage')
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    
    youtube_data = []
    for article in alphavantage_data:
        search_response = youtube.search().list(
            q=article['title'],
            part='snippet',
            maxResults=10,
            type='video'
        ).execute()
        
        for item in search_response.get('items', []):
            video_id = item['id']['videoId']
            comments = get_video_comments(youtube, video_id)
            youtube_data.append({
                'article_title': article['title'],
                'video_id': video_id,
                'comments': comments
            })
    
    return youtube_data

# Step 3: Upload to GCS
def upload_to_gcs(**context):
    alphavantage_data = context['task_instance'].xcom_pull(task_ids='fetch_alphavantage')
    youtube_data = context['task_instance'].xcom_pull(task_ids='fetch_youtube')
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    
    # Upload Alphavantage data
    alpha_blob = bucket.blob(f'raw/alphavantage/data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
    alpha_blob.upload_from_string(json.dumps(alphavantage_data))
    
    # Upload YouTube data
    youtube_blob = bucket.blob(f'raw/youtube/data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
    youtube_blob.upload_from_string(json.dumps(youtube_data))

# Create DAG
dag = DAG(
    'financial_sentiment_pipeline',
    default_args=default_args,
    description='Pipeline for financial sentiment analysis',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Define tasks
task_fetch_alphavantage = PythonOperator(
    task_id='fetch_alphavantage',
    python_callable=fetch_alphavantage_data,
    dag=dag
)

task_fetch_youtube = PythonOperator(
    task_id='fetch_youtube',
    python_callable=fetch_youtube_data,
    dag=dag
)

task_upload_to_gcs = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    dag=dag
)

# Create Dataproc cluster
create_cluster = DataprocCreateClusterOperator(
    task_id='create_cluster',
    project_id=GCP_PROJECT_ID,
    cluster_name='sentiment-cluster-{{ ds_nodash }}',
    num_workers=2,
    region='us-central1',
    dag=dag
)

# Submit Sentiment Analysis job
SENTIMENT_ANALYSIS_JOB = {
    'pyspark_job': {
        'main_python_file_uri': f'gs://{GCS_BUCKET}/scripts/sentiment_analysis.py'
    }
}

submit_job = DataprocSubmitJobOperator(
    task_id='submit_sentiment_job',
    project_id=GCP_PROJECT_ID,
    region='us-central1',
    job=SENTIMENT_ANALYSIS_JOB,
    dag=dag
)

# Delete cluster
delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_cluster',
    project_id=GCP_PROJECT_ID,
    cluster_name='sentiment-cluster-{{ ds_nodash }}',
    region='us-central1',
    dag=dag
)

# Load to MongoDB
load_to_mongodb = GCSToMongoDBOperator(
    task_id='load_to_mongodb',
    gcs_bucket=GCS_BUCKET,
    gcs_object=f'processed/sentiment_results_{datetime.now().strftime("%Y%m%d")}.json',
    mongo_uri=MONGO_CONNECTION_STRING,
    mongo_db='financial_sentiment',
    mongo_collection='sentiment_results',
    dag=dag
)

# Set task dependencies
task_fetch_alphavantage >> task_fetch_youtube >> task_upload_to_gcs >> create_cluster >> submit_job >> delete_cluster >> load_to_mongodb
