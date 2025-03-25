import psycopg2
import logging
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
from datetime import datetime, timedelta
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

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

def generate_sentiment_report(days=30, min_confidence=0.8, top_n=10):
    """Generate a report of the top positive sentiment posts per platform
    
    Args:
        days (int): Number of days to look back
        min_confidence (float): Minimum confidence level for sentiment
        top_n (int): Number of top posts to include per platform
        
    Returns:
        bool: True if report was generated successfully
    """
    conn = get_db_connection()
    if not conn:
        logger.error("Database connection failed")
        return False
    
    report_file = "sentiment_report.txt"
    time_window = datetime.now() - timedelta(days=days)
    
    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"PulseCheck Sentiment Report - Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Top {top_n} positive sentiment posts from the last {days} days\n")
            f.write("=" * 80 + "\n\n")
            
            cursor = conn.cursor()
            
            # Discord posts
            f.write("DISCORD POSTS:\n")
            f.write("-" * 80 + "\n\n")
            
            cursor.execute("""
                SELECT sa.message_id, sa.sentiment, sa.confidence, sa.topics, 
                       dm.content, dm.timestamp, dm.user_id
                FROM sentiment_analysis sa
                JOIN discord_messages dm ON sa.message_id = dm.message_id
                WHERE sa.platform = 'discord' 
                  AND sa.sentiment = 'POSITIVE' 
                  AND sa.confidence > %s
                  AND dm.timestamp > %s
                ORDER BY sa.confidence DESC, dm.timestamp DESC
                LIMIT %s
            """, (min_confidence, time_window, top_n))
            
            discord_posts = cursor.fetchall()
            
            if not discord_posts:
                f.write("No Discord posts found.\n\n")
            else:
                for i, (msg_id, sentiment, confidence, topics, content, timestamp, user_id) in enumerate(discord_posts, 1):
                    f.write(f"#{i} Message ID: {msg_id}\n")
                    f.write(f"Timestamp: {timestamp}\n")
                    f.write(f"User ID: {user_id}\n")
                    f.write(f"Sentiment: {sentiment} (Confidence: {confidence:.2f})\n")
                    f.write(f"Topics: {topics or 'N/A'}\n")
                    f.write(f"Content: {content}\n")
                    f.write("-" * 80 + "\n\n")
            
            # Bluesky posts
            f.write("\nBLUESKY POSTS:\n")
            f.write("-" * 80 + "\n\n")
            
            cursor.execute("""
                SELECT sa.message_id, sa.sentiment, sa.confidence, sa.topics, 
                       bp.content, bp.timestamp, bp.user_did
                FROM sentiment_analysis sa
                JOIN bluesky_posts bp ON sa.message_id = bp.post_id
                WHERE sa.platform = 'bluesky' 
                  AND sa.sentiment = 'POSITIVE' 
                  AND sa.confidence > %s
                  AND bp.timestamp > %s
                ORDER BY sa.confidence DESC, bp.timestamp DESC
                LIMIT %s
            """, (min_confidence, time_window, top_n))
            
            bluesky_posts = cursor.fetchall()
            
            if not bluesky_posts:
                f.write("No Bluesky posts found.\n\n")
            else:
                for i, (post_id, sentiment, confidence, topics, content, timestamp, user_did) in enumerate(bluesky_posts, 1):
                    f.write(f"#{i} Post ID: {post_id}\n")
                    f.write(f"Timestamp: {timestamp}\n")
                    f.write(f"User DID: {user_did}\n")
                    f.write(f"Sentiment: {sentiment} (Confidence: {confidence:.2f})\n")
                    f.write(f"Topics: {topics or 'N/A'}\n")
                    f.write(f"Content: {content}\n")
                    f.write("-" * 80 + "\n\n")
            
            f.write("\nEnd of Report\n")
        
        logger.info(f"Sentiment report generated: {os.path.abspath(report_file)}")
        
        # Print report contents
        with open(report_file, 'r', encoding='utf-8') as f:
            report_content = f.read()
            print(report_content)
            
        return True
    except Exception as e:
        logger.error(f"Error generating sentiment report: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    generate_sentiment_report() 