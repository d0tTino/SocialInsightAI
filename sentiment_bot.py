import tweepy
from atproto import Client
import psycopg2
from config import BLUESKY_USERNAME, BLUESKY_PASSWORD, X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
import logging
from datetime import datetime, timedelta
import argparse
import sys

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

def authenticate_platforms(target_platforms=None):
    """Authenticate with X and Bluesky platforms, returning success status
    
    Args:
        target_platforms (list): List of platforms to authenticate with ('x', 'bluesky', or both)
    """
    global x_client, bsky_client
    platforms_available = {"x": False, "bluesky": False}
    
    # Skip authentication for platforms not in target_platforms
    if target_platforms and 'x' not in target_platforms:
        logger.info("X platform not selected, skipping authentication")
    else:
        # X setup
        try:
            if all([X_API_KEY != "YOUR_X_API_KEY_HERE", 
                    X_API_SECRET != "YOUR_X_API_SECRET_HERE",
                    X_ACCESS_TOKEN != "YOUR_X_ACCESS_TOKEN_HERE", 
                    X_ACCESS_TOKEN_SECRET != "YOUR_X_ACCESS_TOKEN_SECRET_HERE"]):
                x_client = tweepy.Client(
                    consumer_key=X_API_KEY, 
                    consumer_secret=X_API_SECRET,
                    access_token=X_ACCESS_TOKEN, 
                    access_token_secret=X_ACCESS_TOKEN_SECRET
                )
                platforms_available["x"] = True
                logger.info("Successfully authenticated with X")
            else:
                logger.warning("X credentials not configured. X posting will be skipped.")
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
    platforms = authenticate_platforms(target_platforms)
    
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
        cursor = conn.cursor()
        # Use 7 days instead of 1 day
        seven_days_ago = datetime.now() - timedelta(days=7)
        
        # Query database for positive sentiment messages
        cursor.execute("""
            SELECT sa.message_id, sa.platform, sa.sentiment, sa.confidence, COALESCE(dm.content, bp.content)
            FROM sentiment_analysis sa
            LEFT JOIN discord_messages dm ON sa.message_id = dm.message_id AND sa.platform = 'discord'
            LEFT JOIN bluesky_posts bp ON sa.message_id = bp.post_id AND sa.platform = 'bluesky'
            WHERE sa.sentiment = 'POSITIVE' AND sa.confidence > 0.8 AND COALESCE(dm.timestamp, bp.timestamp) > %s
            ORDER BY COALESCE(dm.timestamp, bp.timestamp) DESC
            LIMIT %s
        """, (seven_days_ago, platform_limit * 2))
        
        posts = cursor.fetchall()
        if not posts:
            logger.info("No positive sentiment posts found within the time range.")
            cursor.close()
            conn.close()
            return True
        
        x_count, bsky_count = 0, 0
        
        for msg_id, platform, sentiment, confidence, content in posts:
            if not content:
                continue
                
            snippet = content[:50] + '...' if len(content) > 50 else content
            # Updated message format
            message = f"PulseCheck Alert: {platform.capitalize()} buzzing with positivity: '{snippet}' (Score: {confidence:.2f})"
            
            # Post to X
            if platform == 'discord' and x_count < platform_limit and platforms["x"]:
                if dry_run:
                    logger.info(f"DRY-RUN: Would post to X: {message}")
                else:
                    try:
                        x_client.create_tweet(text=message)
                        logger.info(f"Posted to X: {message}")
                    except Exception as e:
                        logger.error(f"Error posting to X: {e}")
                x_count += 1
                
            # Post to Bluesky
            elif platform == 'bluesky' and bsky_count < platform_limit and platforms["bluesky"]:
                if dry_run:
                    logger.info(f"DRY-RUN: Would post to Bluesky: {message}")
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
    args = parser.parse_args()
    
    # Convert platform argument to a list
    target_platforms = None if args.platform == 'both' else [args.platform]
    
    success = post_sentiment_summary(dry_run=args.dry_run, target_platforms=target_platforms)
    sys.exit(0 if success else 1) 