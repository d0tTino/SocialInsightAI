import psycopg2
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
import logging

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

def check_sentiment_distribution():
    """Check the distribution of sentiment in the database"""
    conn = get_db_connection()
    if not conn:
        logger.error("Database connection failed")
        return

    cursor = conn.cursor()
    
    # Get total count
    cursor.execute("SELECT COUNT(*) FROM sentiment_analysis")
    total = cursor.fetchone()[0]
    logger.info(f"Total sentiment records: {total}")
    
    # Get count by sentiment
    cursor.execute("SELECT sentiment, COUNT(*) FROM sentiment_analysis GROUP BY sentiment")
    sentiment_counts = cursor.fetchall()
    for sentiment, count in sentiment_counts:
        percentage = (count / total) * 100 if total > 0 else 0
        logger.info(f"{sentiment}: {count} records ({percentage:.2f}%)")
    
    # Get count of positive sentiment with high confidence
    cursor.execute("SELECT COUNT(*) FROM sentiment_analysis WHERE sentiment = 'POSITIVE' AND confidence > 0.8")
    high_confidence_positive = cursor.fetchone()[0]
    logger.info(f"Positive sentiment with confidence > 0.8: {high_confidence_positive}")
    
    # Get most recent positive sentiment with high confidence
    cursor.execute("""
        SELECT sa.message_id, sa.platform, sa.sentiment, sa.confidence, 
               COALESCE(dm.content, bp.content), COALESCE(dm.timestamp, bp.timestamp)
        FROM sentiment_analysis sa
        LEFT JOIN discord_messages dm ON sa.message_id = dm.message_id AND sa.platform = 'discord'
        LEFT JOIN bluesky_posts bp ON sa.message_id = bp.post_id AND sa.platform = 'bluesky'
        WHERE sa.sentiment = 'POSITIVE' AND sa.confidence > 0.8
        ORDER BY COALESCE(dm.timestamp, bp.timestamp) DESC
        LIMIT 5
    """)
    recent_positive = cursor.fetchall()
    
    logger.info("Recent positive sentiment messages with high confidence:")
    for i, (msg_id, platform, sentiment, confidence, content, timestamp) in enumerate(recent_positive, 1):
        snippet = content[:50] + '...' if content and len(content) > 50 else content
        logger.info(f"{i}. [{platform}] {snippet} (Confidence: {confidence:.2f}, Timestamp: {timestamp})")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_sentiment_distribution() 