import psycopg2
from db import get_db_connection
import logging
import sys
import subprocess

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Check and install required dependencies
def install_dependencies():
    logger.info("Checking and installing required dependencies")
    try:
        # Install numpy<2 for compatibility
        subprocess.check_call([sys.executable, "-m", "pip", "install", "numpy<2", "--quiet"])
        # Install transformers after numpy is configured
        subprocess.check_call([sys.executable, "-m", "pip", "install", "transformers", "--quiet"])
        logger.info("Dependencies installed successfully")
    except Exception as e:
        logger.error(f"Error installing dependencies: {e}")
        sys.exit(1)

# Now import transformers after ensuring correct numpy version
install_dependencies()
from transformers import pipeline

# Load sentiment analysis model
try:
    sentiment_classifier = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')
    logger.info("Sentiment analysis model loaded successfully")
except Exception as e:
    logger.error(f"Error loading sentiment analysis model: {e}")
    sys.exit(1)

def create_sentiment_table():
    conn = get_db_connection()
    if not conn:
        logger.error("Database connection failed")
        return
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sentiment_analysis (
            id SERIAL PRIMARY KEY,
            message_id VARCHAR(100),  -- Discord or Bluesky ID
            platform VARCHAR(20),     -- 'discord' or 'bluesky'
            sentiment VARCHAR(20),    -- 'POSITIVE' or 'NEGATIVE'
            confidence FLOAT,
            UNIQUE (message_id, platform)
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Sentiment analysis table created or verified")

def analyze_and_store(batch_size=100):
    conn = get_db_connection()
    if not conn:
        logger.error("Database connection failed")
        return
    cursor = conn.cursor()
    try:
        # Discord messages
        cursor.execute("SELECT message_id, content FROM discord_messages WHERE content IS NOT NULL")
        discord_rows = cursor.fetchall()
        logger.info(f"Found {len(discord_rows)} Discord messages to analyze")
        
        for i in range(0, len(discord_rows), batch_size):
            batch = discord_rows[i:i + batch_size]
            for msg_id, content in batch:
                try:
                    # Skip empty content
                    if not content or len(content.strip()) == 0:
                        continue
                    
                    result = sentiment_classifier(content)[0]
                    cursor.execute("""
                        INSERT INTO sentiment_analysis (message_id, platform, sentiment, confidence)
                        VALUES (%s, 'discord', %s, %s)
                        ON CONFLICT (message_id, platform) DO NOTHING;
                    """, (msg_id, result['label'], result['score']))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error analyzing Discord message {msg_id}: {e}")
            logger.info(f"Processed Discord batch {i//batch_size + 1} - {len(batch)} messages")
        
        # Bluesky posts
        cursor.execute("SELECT post_id, content FROM bluesky_posts WHERE content IS NOT NULL")
        bluesky_rows = cursor.fetchall()
        logger.info(f"Found {len(bluesky_rows)} Bluesky posts to analyze")
        
        for i in range(0, len(bluesky_rows), batch_size):
            batch = bluesky_rows[i:i + batch_size]
            for post_id, content in batch:
                try:
                    # Skip empty content
                    if not content or len(content.strip()) == 0:
                        continue
                        
                    result = sentiment_classifier(content)[0]
                    cursor.execute("""
                        INSERT INTO sentiment_analysis (message_id, platform, sentiment, confidence)
                        VALUES (%s, 'bluesky', %s, %s)
                        ON CONFLICT (message_id, platform) DO NOTHING;
                    """, (post_id, result['label'], result['score']))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error analyzing Bluesky post {post_id}: {e}")
            logger.info(f"Processed Bluesky batch {i//batch_size + 1} - {len(batch)} posts")
    except Exception as e:
        logger.error(f"Error during sentiment analysis: {e}")
    finally:
        cursor.close()
        conn.close()
    logger.info("Sentiment analysis complete")

if __name__ == "__main__":
    create_sentiment_table()
    analyze_and_store() 