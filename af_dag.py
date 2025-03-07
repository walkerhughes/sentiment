import heapq
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from google.cloud import storage
import requests
import json
import googleapiclient.discovery
from pymongo import MongoClient
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from custom_operators import CustomGCSToMongoDBOperator

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

def youtube_search(query, max_results=10):

    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)
    
    # Call the search.list method to retrieve results matching the query
    request = youtube.search().list(
        part="snippet",
        q=query,
        type="video",
        maxResults=max_results
    )
    response = request.execute()
    
    # Extract video IDs, titles, and publication dates from the response
    videos = []
    for item in response.get("items", []):
        video_data = {
            "video_id": item["id"]["videoId"],
            "title": item["snippet"]["title"],
            "published_at": item["snippet"]["publishedAt"]
        }
        videos.append(video_data)
    
    return videos


def get_video_comments(video_id, max_results=100):
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)

    comments = []
    next_page_token = None

    while len(comments) < max_results:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=min(100, max_results - len(comments)),
            textFormat="plainText",
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response.get("items", []):
            comment = item["snippet"]["topLevelComment"]["snippet"]
            comment_data = {
                "author": comment["authorDisplayName"],
                "text": comment["textDisplay"],
                "likes": comment["likeCount"],
                "published_at": comment["publishedAt"]
            }
            comments.append(comment_data)

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return comments

def get_yt_comments_for_all_tickers(
    tickers: list[str] = ["AAPL"],
    max_video_results: int = 2,
    max_comments: int = 2
) -> list[dict]:
    
    parsed_video_data = []
    search_template = """latest news for {ticker} stock"""
    
    for ticker in tickers:
        for ticker_data in youtube_search(
                query=search_template.format(ticker=ticker),
                max_results=max_video_results
            ):

            comments = get_video_comments(ticker_data["video_id"], max_results=max_comments)
            top_comments = heapq.nlargest(max_comments, comments, key=lambda item: item["likes"])
            
            parsed_video_data.append({
                "ticker": ticker,
                "video_id": ticker_data["video_id"],
                "video_title": ticker_data["title"],
                "published_at": ticker_data["published_at"],
                "top_comments": top_comments
            })
    
    return parsed_video_data

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

# Add this function to your DAG file
def transfer_gcs_to_mongodb(**context):
    # Get data from GCS
    gcs_hook = GCSHook()
    bucket = context['gcs_bucket']
    object_name = context['gcs_object']
    data_string = gcs_hook.download(bucket_name=bucket, object_name=object_name)
    data = json.loads(data_string)
    
    # Upload to MongoDB
    mongo_hook = MongoHook(conn_id='mongo_default')
    mongo_hook.insert_many(
        mongo_collection=context['mongo_collection'],
        mongo_db=context['mongo_db'],
        docs=data
    )
    return f"Transferred {len(data)} documents to MongoDB"

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

# Replace the GCSToMongoDBOperator with PythonOperator
load_to_mongodb = CustomGCSToMongoDBOperator(
    task_id='load_to_mongodb',
    gcs_bucket=GCS_BUCKET,
    gcs_object=f'processed/sentiment_results_{datetime.now().strftime("%Y%m%d")}.json',
    mongo_db='financial_sentiment',
    mongo_collection='sentiment_results',
    dag=dag
)

# Set task dependencies
task_fetch_alphavantage >> task_fetch_youtube >> task_upload_to_gcs >> create_cluster >> submit_job >> delete_cluster >> load_to_mongodb
