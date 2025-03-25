import tweepy
from atproto import Client
import psycopg2
from config import BLUESKY_USERNAME, BLUESKY_PASSWORD, X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
import logging
from datetime import datetime, timedelta
import argparse
import sys
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from collections import Counter
import re
import string

# Download necessary NLTK resources
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('punkt')
    nltk.download('stopwords')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize clients as None
x_client = None
bsky_client = None

def get_db_connection():
    """Create a database connection to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def extract_topics(text, num_topics=2):
    """Extract key topics from text using frequency analysis
    
    Args:
        text (str): Text to analyze
        num_topics (int): Maximum number of topics to extract
        
    Returns:
        str: Comma-separated list of topics
    """
    if not text or len(text) < 5:
        return "general"
    
    # Clean the text
    text = text.lower()
    text = re.sub(r'http\S+', '', text)  # Remove URLs
    text = text.translate(str.maketrans('', '', string.punctuation))  # Remove punctuation
    
    # Tokenize and remove stopwords
    stop_words = set(stopwords.words('english'))
    additional_stops = {'just', 'like', 'get', 'got', 'know', 'yeah', 'dont', 'thats', 'really', 'going', 'think', 'said'}
    stop_words.update(additional_stops)
    
    tokens = word_tokenize(text)
    tokens = [word for word in tokens if word not in stop_words and len(word) > 3]
    
    # Count word frequencies
    counter = Counter(tokens)
    
    # Get most common words as topics
    topics = [word for word, count in counter.most_common(num_topics) if count > 0]
    
    # Return comma-separated topics or "general" if none found
    result = ", ".join(topics)
    
    # Ensure result doesn't exceed 95 characters
    if len(result) > 95:
        result = result[:95]
    
    return result if topics else "general"

def update_topics_in_database():
    """Update the topics column for all sentiment analysis records"""
    conn = get_db_connection()
    if not conn:
        logger.error("Database connection failed")
        return False
    
    try:
        cursor = conn.cursor()
        
        # First set all topics to NULL
        cursor.execute("UPDATE sentiment_analysis SET topics = NULL WHERE topics IS NULL")
        
        # Process Discord messages
        cursor.execute("""
            SELECT sa.id, dm.content 
            FROM sentiment_analysis sa
            JOIN discord_messages dm ON sa.message_id = dm.message_id
            WHERE sa.platform = 'discord' AND sa.topics IS NULL
            LIMIT 10000
        """)
        discord_messages = cursor.fetchall()
        
        logger.info(f"Updating topics for {len(discord_messages)} Discord messages")
        for record_id, content in discord_messages:
            if content:
                topics = extract_topics(content)
                cursor.execute("UPDATE sentiment_analysis SET topics = %s WHERE id = %s", (topics, record_id))
        
        # Process Bluesky posts
        cursor.execute("""
            SELECT sa.id, bp.content 
            FROM sentiment_analysis sa
            JOIN bluesky_posts bp ON sa.message_id = bp.post_id
            WHERE sa.platform = 'bluesky' AND sa.topics IS NULL
            LIMIT 10000
        """)
        bluesky_posts = cursor.fetchall()
        
        logger.info(f"Updating topics for {len(bluesky_posts)} Bluesky posts")
        for record_id, content in bluesky_posts:
            if content:
                topics = extract_topics(content)
                cursor.execute("UPDATE sentiment_analysis SET topics = %s WHERE id = %s", (topics, record_id))
        
        conn.commit()
        logger.info(f"Updated topics for {len(discord_messages) + len(bluesky_posts)} messages")
        return True
    except Exception as e:
        logger.error(f"Error updating topics: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def authenticate_platforms(target_platforms=None, dry_run=False):
    """Authenticate with X and Bluesky platforms, returning success status
    
    Args:
        target_platforms (list): List of platforms to authenticate with ('x', 'bluesky', or both)
        dry_run (bool): If True, allow partial credentials for dry-run mode
    """
    global x_client, bsky_client
    platforms_available = {"x": False, "bluesky": False}
    
    # Skip authentication for platforms not in target_platforms
    if target_platforms and 'x' not in target_platforms:
        logger.info("X platform not selected, skipping authentication")
    else:
        # X setup
        try:
            # Only check if credentials are provided, not placeholder values
            if all([X_API_KEY != "YOUR_X_API_KEY_HERE", 
                    X_API_SECRET != "YOUR_X_API_SECRET_HERE",
                    X_ACCESS_TOKEN != "YOUR_X_ACCESS_TOKEN_HERE", 
                    X_ACCESS_TOKEN_SECRET != "YOUR_X_ACCESS_TOKEN_SECRET_HERE"]):
                try:
                    x_client = tweepy.Client(
                        consumer_key=X_API_KEY, 
                        consumer_secret=X_API_SECRET,
                        access_token=X_ACCESS_TOKEN, 
                        access_token_secret=X_ACCESS_TOKEN_SECRET
                    )
                    platforms_available["x"] = True
                    logger.info("Successfully authenticated with X")
                except Exception as e:
                    logger.error(f"X authentication error with provided credentials: {e}")
                    if dry_run:
                        platforms_available["x"] = True
                        logger.info("Using placeholder X credentials for dry run despite authentication error")
            else:
                logger.warning("X credentials not fully configured. X posting will be skipped in live mode.")
                if dry_run:
                    platforms_available["x"] = True
                    logger.info("Using placeholder X credentials for dry run")
        except Exception as e:
            logger.error(f"X authentication error: {e}")
    
    if target_platforms and 'bluesky' not in target_platforms:
        logger.info("Bluesky platform not selected, skipping authentication")
    else:
        # Bluesky setup
        try:
            bsky_client = Client()
            bsky_client.login(BLUESKY_USERNAME, BLUESKY_PASSWORD)
            platforms_available["bluesky"] = True
            logger.info("Successfully authenticated with Bluesky")
        except Exception as e:
            logger.error(f"Bluesky authentication error: {e}")
    
    return platforms_available

def post_sentiment_summary(platform_limit=5, dry_run=False, target_platforms=None):
    """Post positive sentiment insights to social media platforms
    
    Args:
        platform_limit (int): Maximum number of posts per platform
        dry_run (bool): If True, log posts without sending them
        target_platforms (list): List of platforms to post to ('x', 'bluesky', or both)
    """
    # Authenticate with platforms
    platforms = authenticate_platforms(target_platforms, dry_run)
    
    active_platforms = [p for p, available in platforms.items() if available]
    if not active_platforms:
        logger.error("No platforms available to post to. Exiting.")
        return False
    logger.info(f"Active platforms: {', '.join(active_platforms)}")
    
    # Connect to database
    conn = get_db_connection()
    if not conn:
        logger.error("Database connection failed. Exiting.")
        return False
    
    try:
        # First update topics for messages without topics
        logger.info("Checking and updating topics for messages")
        update_topics_in_database()
        
        cursor = conn.cursor()
        # Use a much wider time window (30 days) to find more posts
        discord_time_window = datetime.now() - timedelta(days=30)
        bluesky_time_window = datetime.now() - timedelta(days=7)
        
        # First query for Discord posts
        if not target_platforms or 'x' in target_platforms:
            cursor.execute("""
                SELECT sa.message_id, sa.platform, sa.sentiment, sa.confidence, dm.content, dm.timestamp, sa.topics
                FROM sentiment_analysis sa
                JOIN discord_messages dm ON sa.message_id = dm.message_id
                WHERE sa.platform = 'discord' AND sa.sentiment = 'POSITIVE' AND sa.confidence > 0.8 
                  AND dm.timestamp > %s
                ORDER BY dm.timestamp DESC
                LIMIT %s
            """, (discord_time_window, platform_limit))
            
            discord_posts = cursor.fetchall()
            logger.info(f"Found {len(discord_posts)} Discord posts with positive sentiment")
        else:
            discord_posts = []
            
        # Then query for Bluesky posts
        if not target_platforms or 'bluesky' in target_platforms:
            cursor.execute("""
                SELECT sa.message_id, sa.platform, sa.sentiment, sa.confidence, bp.content, bp.timestamp, sa.topics
                FROM sentiment_analysis sa
                JOIN bluesky_posts bp ON sa.message_id = bp.post_id
                WHERE sa.platform = 'bluesky' AND sa.sentiment = 'POSITIVE' AND sa.confidence > 0.8 
                  AND bp.timestamp > %s
                ORDER BY bp.timestamp DESC
                LIMIT %s
            """, (bluesky_time_window, platform_limit))
            
            bluesky_posts = cursor.fetchall()
            logger.info(f"Found {len(bluesky_posts)} Bluesky posts with positive sentiment")
        else:
            bluesky_posts = []
            
        # Process Discord posts
        x_count = 0
        for msg_id, platform, sentiment, confidence, content, timestamp, topics in discord_posts:
            if not content:
                continue
            
            # Get topics if not already present
            if not topics:
                topics = extract_topics(content)
                # Update database with topics
                cursor.execute("""
                    UPDATE sentiment_analysis 
                    SET topics = %s 
                    WHERE message_id = %s AND platform = %s
                """, (topics, msg_id, platform))
                conn.commit()
                
            snippet = content[:50] + '...' if len(content) > 50 else content
            topic_text = f"about {topics}" if topics else ""
            message = f"PulseCheck Alert: {platform.capitalize()} buzzing {topic_text}: '{snippet}' (Score: {confidence:.2f})"
            
            if platforms["x"]:
                if dry_run:
                    logger.info(f"DRY-RUN: Would post to X: {message}")
                    logger.info(f"DRY-RUN: Full content: {content}")
                else:
                    try:
                        x_client.create_tweet(text=message)
                        logger.info(f"Posted to X: {message}")
                    except Exception as e:
                        logger.error(f"Error posting to X: {e}")
                x_count += 1
            
        # Process Bluesky posts
        bsky_count = 0
        for msg_id, platform, sentiment, confidence, content, timestamp, topics in bluesky_posts:
            if not content:
                continue
                
            # Get topics if not already present
            if not topics:
                topics = extract_topics(content)
                # Update database with topics
                cursor.execute("""
                    UPDATE sentiment_analysis 
                    SET topics = %s 
                    WHERE message_id = %s AND platform = %s
                """, (topics, msg_id, platform))
                conn.commit()
                
            snippet = content[:50] + '...' if len(content) > 50 else content
            topic_text = f"about {topics}" if topics else ""
            message = f"PulseCheck Alert: {platform.capitalize()} buzzing {topic_text}: '{snippet}' (Score: {confidence:.2f})"
            
            if platforms["bluesky"]:
                if dry_run:
                    logger.info(f"DRY-RUN: Would post to Bluesky: {message}")
                    logger.info(f"DRY-RUN: Full content: {content}")
                else:
                    try:
                        bsky_client.send_post(text=message)
                        logger.info(f"Posted to Bluesky: {message}")
                    except Exception as e:
                        logger.error(f"Error posting to Bluesky: {e}")
                bsky_count += 1
        
        logger.info(f"Run complete: {x_count} X posts, {bsky_count} Bluesky posts {'(dry run)' if dry_run else ''}")
        
    except Exception as e:
        logger.error(f"Error processing sentiment data: {e}")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()
    
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Post sentiment summaries to X and Bluesky')
    parser.add_argument('--dry-run', action='store_true', help='Log posts without sending them')
    parser.add_argument('--platform', choices=['x', 'bluesky', 'both'], default='both', 
                        help='Platform to post to (default: both)')
    parser.add_argument('--count', type=int, default=5, 
                        help='Number of posts per platform (default: 5)')
    args = parser.parse_args()
    
    # Convert platform argument to a list
    target_platforms = None if args.platform == 'both' else [args.platform]
    
    success = post_sentiment_summary(platform_limit=args.count, dry_run=args.dry_run, target_platforms=target_platforms)
    sys.exit(0 if success else 1) 