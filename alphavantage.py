import requests
import os
import json
from dotenv import load_dotenv
import time
from datetime import datetime
from google.cloud import storage

# Load environment variables
load_dotenv(dotenv_path='/User/bensunshine/repos/msds697/project/.env')

API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')

tickers = ['AAPL', 'NVDA', 'MSFT', 'AMZN', 'GOOG', 'META', 'TSLA']
size = 1000
max_date_time = '20220101T0001'

# List to store all results
all_results = []

# Get current timestamp
timestamp = datetime.utcnow().isoformat()

for ticker in tickers:
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&time_from={max_date_time}&limit={size}&apikey={API_KEY}'
    r = requests.get(url)
    data = r.json()
    
    # Format each article with additional metadata
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
    

filename = f'news_sentiment_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

# Save to local file (can be uploaded to GCS)
with open(filename, 'w') as f:
    json.dump(all_results, f, indent=2)

print(f"Data saved to {filename}")


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client.from_service_account_json('google_service_account_key.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

# Upload to GCS
bucket_name = 'alphaverse_news'
destination_blob_name = f'{filename}'
upload_to_gcs(bucket_name, filename, destination_blob_name)
