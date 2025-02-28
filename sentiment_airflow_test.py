#!/usr/bin/env python
# Pure Spark Sentiment Analysis Pipeline for YouTube Comments
# For running on AWS EMR with data from Google Cloud Storage

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, broadcast, count, when
from pyspark.sql.types import StringType, FloatType
import argparse
import logging
import os
from transformers import pipeline

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def initialize_spark_emr():
    """Initialize Spark session optimized for EMR with GCS connectivity"""
    logger.info("Initializing Spark session with GCS support")
    
    return SparkSession.builder \
        .appName("YouTubeCommentSentimentAnalysis") \
        .config("spark.driver.memory", "16G") \
        .config("spark.executor.memory", "16G") \
        .config("spark.executor.cores", "4") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.jars.packages", "com.google.cloud:google-cloud-storage:2.20.1") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()

def get_sentiment_score(text):
    """
    Get sentiment score for text using the global sentiment_analyzer
    Returns a float value in range [-1, 1]
    """
    if not text or len(text.strip()) == 0:
        return 0.0
    
    try:
        # Limit text length to avoid token limit issues
        limited_text = text[:512] if len(text) > 512 else text
        
        # Use the sentiment analyzer (accessible from the UDF)
        result = sentiment_analyzer(limited_text)
        
        # Calculate a weighted score in the range [-1, 1]
        # Most Hugging Face models return scores in different formats
        # We need to adapt this based on the specific model used
        
        # For models with 'label' and 'score' in output
        if isinstance(result, list) and 'label' in result[0]:
            label = result[0]['label'].lower()
            score = result[0]['score']
            
            if 'positive' in label:
                return float(score)
            elif 'negative' in label:
                return float(-score)
            else:
                return 0.0
                
        # For models with 'positive', 'negative', 'neutral' scores
        elif isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict):
            # Adjust based on specific model output format
            scores_dict = {item['label'].lower(): item['score'] for item in result}
            
            if 'positive' in scores_dict and 'negative' in scores_dict:
                # Calculate weighted score: positive - negative will be in range [-1, 1]
                return float(scores_dict.get('positive', 0) - scores_dict.get('negative', 0))
            
        # Default fallback
        return 0.0
            
    except Exception as e:
        logger.warning(f"Error analyzing sentiment: {str(e)}")
        return 0.0

def get_sentiment_label(score):
    """Convert a sentiment score to a label"""
    if score >= 0.2:
        return "positive"
    elif score <= -0.2:
        return "negative"
    else:
        return "neutral"

def process_nested_json(df):
    """Process the nested YouTube comments JSON structure"""
    logger.info("Processing nested JSON structure")
    
    if "top_comments" in df.columns:
        # Explode the nested "top_comments" array to get individual comments
        comments_df = df.select(
            col("video_id"),
            col("title"),
            col("published_at").alias("video_published_at"),
            col("ticker"),
            explode(col("top_comments")).alias("comment")
        )
        
        # Extract fields from the comment struct
        return comments_df.select(
            col("video_id"),
            col("title"),
            col("video_published_at"),
            col("ticker"),
            col("comment.author"),
            col("comment.text"),
            col("comment.likes"),
            col("comment.published_at").alias("comment_published_at")
        )
    else:
        logger.warning("Input data does not contain 'top_comments' field. Check data format.")
        return df

def analyze_sentiment_batch(df, batch_size=1000):
    """
    Apply sentiment analysis in batches to avoid memory issues
    This is a pure Spark approach that doesn't use pandas
    """
    logger.info("Applying sentiment analysis")
    
    # Register UDFs
    score_udf = udf(get_sentiment_score, FloatType())
    label_udf = udf(get_sentiment_label, StringType())
    
    # Get total row count
    row_count = df.count()
    logger.info(f"Processing {row_count} comments")
    
    # Process in batches to avoid memory issues
    current_offset = 0
    all_results = None
    
    while current_offset < row_count:
        # Get the current batch
        current_batch = df.limit(batch_size).offset(current_offset)
        
        # Apply sentiment analysis to this batch
        current_results = current_batch.withColumn("sentiment_score", score_udf(col("text"))) \
                                      .withColumn("sentiment", label_udf(col("sentiment_score")))
        
        # Append to results
        if all_results is None:
            all_results = current_results
        else:
            all_results = all_results.union(current_results)
        
        # Update offset for next batch
        current_offset += batch_size
        logger.info(f"Processed {min(current_offset, row_count)}/{row_count} comments")
    
    return all_results

def print_stats(sentiment_df):
    """Print statistics about the sentiment analysis results"""
    logger.info("Calculating sentiment statistics")
    
    # Calculate sentiment distribution
    stats = sentiment_df.select(
        count("*").alias("total_comments"),
        count(when(col("sentiment") == "positive", True)).alias("positive_comments"),
        count(when(col("sentiment") == "negative", True)).alias("negative_comments"),
        count(when(col("sentiment") == "neutral", True)).alias("neutral_comments")
    ).collect()[0]
    
    logger.info(f"Total Comments: {stats['total_comments']}")
    
    if stats['total_comments'] > 0:
        logger.info(f"Positive Comments: {stats['positive_comments']} " +
                    f"({stats['positive_comments']/stats['total_comments']*100:.1f}%)")
        logger.info(f"Negative Comments: {stats['negative_comments']} " +
                    f"({stats['negative_comments']/stats['total_comments']*100:.1f}%)")
        logger.info(f"Neutral Comments: {stats['neutral_comments']} " +
                    f"({stats['neutral_comments']/stats['total_comments']*100:.1f}%)")
    
    # Also calculate per-ticker sentiment
    if "ticker" in sentiment_df.columns:
        logger.info("Sentiment breakdown by ticker:")
        sentiment_df.groupBy("ticker", "sentiment").count().orderBy("ticker", "sentiment").show()

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='YouTube Comment Sentiment Analysis on EMR')
    parser.add_argument('--input', required=True, help='Input path (GCS or S3)')
    parser.add_argument('--output', required=True, help='Output path (GCS or S3)')
    parser.add_argument('--gcp-key', help='Path to GCP service account key file (if needed)')
    parser.add_argument('--model', default='distilbert-base-uncased-finetuned-sst-2-english', 
                       help='Hugging Face model name')
    parser.add_argument('--batch-size', type=int, default=1000, 
                       help='Batch size for processing')
    parser.add_argument('--tickers', help='Comma-separated list of tickers to filter for')
    args = parser.parse_args()
    
    # Initialize Spark
    spark = initialize_spark_emr()
    
    # Set GCP credentials if provided
    if args.gcp_key:
        logger.info(f"Setting GCP credentials from {args.gcp_key}")
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("google.cloud.auth.service.account.json.keyfile", args.gcp_key)
    
    # Initialize sentiment analyzer (globally)
    global sentiment_analyzer
    logger.info(f"Loading Hugging Face model: {args.model}")
    sentiment_analyzer = pipeline(
        task="sentiment-analysis",
        model=args.model,
        tokenizer=args.model
    )
    
    # Log Spark application info
    logger.info(f"Spark application ID: {spark.sparkContext.applicationId}")
    logger.info(f"Processing input from: {args.input}")
    logger.info(f"Writing output to: {args.output}")
    
    # Read input data
    logger.info("Reading input data")
    df = spark.read.option("multiline", "true").json(args.input)
    
    # Process the nested JSON structure
    processed_df = process_nested_json(df)
    
    # Filter by ticker if requested
    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(',')]
        logger.info(f"Filtering for tickers: {tickers}")
        processed_df = processed_df.filter(col("ticker").isin(tickers))
    
    # Apply sentiment analysis in batches
    sentiment_df = analyze_sentiment_batch(processed_df, args.batch_size)
    
    # Print statistics
    print_stats(sentiment_df)
    
    # Save results in both parquet and CSV formats
    logger.info("Saving results")
    sentiment_df.write.mode("overwrite").parquet(f"{args.output}/parquet")
    
    # Save CSV in a single file (might be slow for large datasets)
    sentiment_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{args.output}/csv")
    
    # Save sentiment summary as a separate file
    summary_df = sentiment_df.groupBy("ticker", "sentiment").count()
    summary_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{args.output}/summary")
    
    logger.info("Sentiment analysis completed successfully")
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()