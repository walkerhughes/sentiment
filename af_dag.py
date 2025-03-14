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
from google.oauth2 import service_account
import ssl

from transformers import pipeline

sentiment_pipeline = pipeline("sentiment-analysis")

# Environment variables
# ALPHA_VANTAGE_API_KEY = 'OU4IFG8WTEFGTA2T'
# ALPHA_VANTAGE_API_KEY = 'E7BHX8V1IM7JXR3S'
ALPHA_VANTAGE_API_KEY = os.environ.get('ALPHA_VANTAGE_API_KEY')

# YOUTUBE_API_KEY = 'AIzaSyA0bEZKUKJ3yhW8hiRnD9FaYTMENc'
# YOUTUBE_API_KEY = 'AIzaSyAYWOqVU0OL2j4iRGT382IyWDwdActKgu4'
# YOUTUBE_API_KEY = 'AIzaSyCekPhJtGexYecVFMqaKSVyXcEbLx_xCB8'
# YOUTUBE_API_KEY = 'AIzaSyAIK8JIxVbDgi5gTyD0eSZKrYul_44Q3G4'
# vitoria
# YOUTUBE_API_KEY = 'AIzaSyA0bEZKUKJ3yhW8hiRnD9FaYTMENc'
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY')
GCP_PROJECT_ID = 'round-music-450621-b5'
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')

GCS_BUCKET = 'alphaverse_news'
MONGO_CONNECTION_STRING = 'mongodb+srv://racisneros:Gq5bz7rZiBKdWDTM@group.pj9wa.mongodb.net/?retryWrites=true&w=majority&appName=group'
SERVICE_ACCOUNT_PATH = os.path.expanduser('~/Downloads/google_service_account_key.json')

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
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
    max_date_time = datetime.now().strftime("%Y%m%dT0001")
    all_results = []
    timestamp = TIMESTAMP
    
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
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
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
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
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
                "published_at": comment["publishedAt"],
                "sentiment_score": sentiment_pipeline(comment["textDisplay"])
            }
            comments.append(comment_data)
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    return comments
def get_yt_comments_for_all_tickers(
    tickers = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOG", "META", "TSLA"],
    max_video_results: int = 10,
    max_comments: int = 50
):
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


def upload_to_gcs(**context):
    alphavantage_data = context['task_instance'].xcom_pull(task_ids='fetch_alphavantage')
    youtube_data = context['task_instance'].xcom_pull(task_ids='fetch_youtube')
    
    timestamp = TIMESTAMP  # Get current timestamp
    
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_PATH
    )
    
    storage_client = storage.Client(
        credentials=credentials, 
        project=GCP_PROJECT_ID
    )
    bucket = storage_client.bucket(GCS_BUCKET)
    
    # Upload Alphavantage data
    alpha_blob = bucket.blob(f'raw/alphavantage/data_{timestamp}.json')
    alpha_blob.upload_from_string(json.dumps(alphavantage_data))
    
    # Upload YouTube data
    youtube_blob = bucket.blob(f'raw/youtube/data_{timestamp}.json')
    youtube_blob.upload_from_string(json.dumps(youtube_data))
    
    return timestamp  # Return the timestamp used



# Modified to accept timestamp from upload_to_gcs
def load_raw_data_to_mongodb(**context):
    timestamp = context['task_instance'].xcom_pull(task_ids='upload_to_gcs')
    
    # Get credentials and create storage client
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_PATH
    )
    
    storage_client = storage.Client(
        credentials=credentials, 
        project=GCP_PROJECT_ID
    )
    bucket = storage_client.bucket(GCS_BUCKET)

    prefix_alpha = f'raw/alphavantage/data_{timestamp}'
    prefix_youtube = f'raw/youtube/data_{timestamp}'
    
    alpha_blobs = list(bucket.list_blobs(prefix=prefix_alpha))
    youtube_blobs = list(bucket.list_blobs(prefix=prefix_youtube))
    # youtube_blobs = list(bucket.list_blobs(prefix='raw/youtube/data_20250305_104643'))
    
    # MongoDB connection setup
    connection_string = MONGO_CONNECTION_STRING
    if '?' in connection_string:
        connection_string += '&tlsAllowInvalidCertificates=true'
    else:
        connection_string += '?tlsAllowInvalidCertificates=true'
    
    print(f"Connecting to MongoDB with: {connection_string}")
    
    # Create client with connection string
    client = MongoClient(connection_string, 
                         connectTimeoutMS=30000,
                         socketTimeoutMS=30000,
                         serverSelectionTimeoutMS=30000)
    
    # Test the connection first
    try:
        client.admin.command('ping')
        print("MongoDB connection successful!")
    except Exception as e:
        print(f"MongoDB connection failed: {str(e)}")
        raise
    
    db = client['financial_sentiment']
    articles_collection = db['raw_articles']
    yt_comments_collection = db['raw_yt_comments']
    
    results = []
    
    # Process Alphavantage data (articles)
    if alpha_blobs:
        latest_alpha_blob = sorted(alpha_blobs, key=lambda x: x.name, reverse=True)[0]
        alpha_data_string = latest_alpha_blob.download_as_text()
        alpha_data = json.loads(alpha_data_string)
        
        # Insert the article data
        if isinstance(alpha_data, list):
            if alpha_data:
                articles_collection.insert_many(alpha_data)
                results.append(f"Loaded {len(alpha_data)} articles to MongoDB")
            else:
                results.append("No article data to insert (empty list)")
        else:
            articles_collection.insert_one(alpha_data)
            results.append("Loaded 1 article to MongoDB")
    else:
        results.append(f"No files found with prefix: {prefix_alpha}")
    
    # Process YouTube data (comments)
    if youtube_blobs:
        latest_youtube_blob = sorted(youtube_blobs, key=lambda x: x.name, reverse=True)[0]
        youtube_data_string = latest_youtube_blob.download_as_text()
        youtube_data = json.loads(youtube_data_string)
        
        # Insert the YouTube comment data
        if isinstance(youtube_data, list):
            if youtube_data:
                yt_comments_collection.insert_many(youtube_data)
                results.append(f"Loaded {len(youtube_data)} YouTube comments to MongoDB")
            else:
                results.append("No YouTube comment data to insert (empty list)")
        else:
            yt_comments_collection.insert_one(youtube_data)
            results.append("Loaded 1 YouTube comment to MongoDB")
    else:
        results.append(f"No files found with prefix: {prefix_youtube}")
    
    return "\n".join(results)

# Add this function to run MongoDB aggregations
def run_mongodb_aggregations(**context):


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
    python_callable=get_yt_comments_for_all_tickers,
    dag=dag
)

task_upload_to_gcs = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    dag=dag
)



# Replace the CustomGCSToMongoDBOperator with PythonOperator
load_to_mongodb = PythonOperator(
    task_id='load_to_mongodb',
    python_callable=load_raw_data_to_mongodb,
    dag=dag
)

# Add the aggregation task
mongodb_aggregations = PythonOperator(
    task_id='mongodb_aggregations',
    python_callable=run_mongodb_aggregations,
    dag=dag
)

# Set task dependencies
task_fetch_alphavantage >> task_fetch_youtube >> task_upload_to_gcs >> load_to_mongodb >> mongodb_aggregations

