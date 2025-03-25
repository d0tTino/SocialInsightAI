import tweepy
from atproto import Client
import psycopg2
import discord
from config import BLUESKY_USERNAME, BLUESKY_PASSWORD, X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DISCORD_TOKEN
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
import time
import json
import os
import asyncio

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
discord_client = None

# File to store processed IDs
PROCESSED_X_IDS_FILE = "processed_x_ids.json"
PROCESSED_DISCORD_IDS_FILE = "processed_discord_ids.json"

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

def ensure_topics_column_exists():
    """Ensure the topics column exists in the sentiment_analysis table"""
    conn = get_db_connection()
    if not conn:
        logger.error("Database connection failed")
        return False
    
    try:
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if the column already exists
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='sentiment_analysis' AND column_name='topics'
        """)
        
        if cursor.fetchone():
            logger.info("Topics column already exists in sentiment_analysis table")
            return True
        
        # Add the topics column
        cursor.execute("""
            ALTER TABLE sentiment_analysis 
            ADD COLUMN topics VARCHAR(100)
        """)
        
        logger.info("Successfully added topics column to sentiment_analysis table")
        return True
    except Exception as e:
        logger.error(f"Error ensuring topics column: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

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

def load_processed_ids(file_path, default=None):
    """Load processed IDs from file"""
    if default is None:
        default = {"ids": []}
    
    if not os.path.exists(file_path):
        return default
    
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return default

def save_processed_ids(data, file_path):
    """Save processed IDs to file"""
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Error saving processed IDs: {e}")

def authenticate_platforms(target_platforms=None, dry_run=False):
    """Authenticate with X and Bluesky platforms, returning success status
    
    Args:
        target_platforms (list): List of platforms to authenticate with ('x', 'bluesky', 'discord', or any)
        dry_run (bool): If True, allow partial credentials for dry-run mode
    """
    global x_client, bsky_client, discord_client
    platforms_available = {"x": False, "bluesky": False, "discord": False}
    
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
                    auth = tweepy.OAuth1UserHandler(
                        consumer_key=X_API_KEY,
                        consumer_secret=X_API_SECRET,
                        access_token=X_ACCESS_TOKEN,
                        access_token_secret=X_ACCESS_TOKEN_SECRET
                    )
                    x_client = tweepy.API(auth)
                    # Test the connection
                    x_client.verify_credentials()
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
    
    if target_platforms and 'discord' not in target_platforms:
        logger.info("Discord platform not selected, skipping authentication")
    else:
        # Discord setup
        try:
            if DISCORD_TOKEN and DISCORD_TOKEN != "YOUR_DISCORD_TOKEN_HERE":
                discord_client = discord.Client(intents=discord.Intents.all())
                platforms_available["discord"] = True
                logger.info("Discord client initialized")
            else:
                logger.warning("Discord token not configured. Discord collection will be skipped.")
                if dry_run:
                    platforms_available["discord"] = True
                    logger.info("Using placeholder Discord credentials for dry run")
        except Exception as e:
            logger.error(f"Discord initialization error: {e}")
    
    return platforms_available

async def collect_discord_messages(channel_id, limit=10, dry_run=True):
    """Collect messages from a specific Discord channel
    
    Args:
        channel_id (int): ID of the channel to collect from
        limit (int): Maximum number of messages to collect
        dry_run (bool): If True, just log without storing
        
    Returns:
        list: Collected messages
    """
    if not discord_client:
        logger.error("Discord client not initialized")
        return []
    
    # For dry run without actual Discord connection, return simulated data
    if dry_run and not discord_client.is_ready():
        logger.info("DRY-RUN: Using simulated Discord messages (client not ready)")
        simulated_messages = [
            {
                "message_id": f"sim-{i}",
                "content": f"This is a simulated Discord message #{i} for testing.",
                "user_id": "123456789",
                "timestamp": datetime.now().isoformat(),
                "channel_id": str(channel_id),
                "guild_id": "987654321"
            } for i in range(1, 4)
        ]
        for msg in simulated_messages:
            logger.info(f"DRY-RUN: Collected from Discord: {msg['content']}")
        return simulated_messages
    
    # Load processed IDs
    processed_data = load_processed_ids(PROCESSED_DISCORD_IDS_FILE, {"ids": []})
    processed_ids = set(processed_data["ids"])
    
    try:
        channel = await discord_client.fetch_channel(channel_id)
        logger.info(f"Connected to Discord channel: {channel.name}")
        
        messages = []
        async for message in channel.history(limit=limit):
            # Skip already processed messages
            if str(message.id) in processed_ids:
                continue
                
            # Skip bot messages
            if message.author.bot:
                continue
                
            # Skip empty messages
            if not message.content.strip():
                continue
            
            messages.append({
                "message_id": str(message.id),
                "content": message.content,
                "user_id": str(message.author.id),
                "timestamp": message.created_at.isoformat(),
                "channel_id": str(channel.id),
                "guild_id": str(channel.guild.id) if hasattr(channel, "guild") else None
            })
            
            # Add to processed IDs
            processed_ids.add(str(message.id))
            
            if dry_run:
                logger.info(f"DRY-RUN: Collected from Discord: {message.content[:100]}{'...' if len(message.content) > 100 else ''}")
        
        # Update processed IDs file
        processed_data["ids"] = list(processed_ids)
        save_processed_ids(processed_data, PROCESSED_DISCORD_IDS_FILE)
        
        logger.info(f"Collected {len(messages)} new Discord messages")
        return messages
    except Exception as e:
        logger.error(f"Error collecting Discord messages: {e}")
        if dry_run:
            # Return simulated data for testing
            logger.info("DRY-RUN: Using simulated Discord messages due to error")
            simulated_messages = [
                {
                    "message_id": f"sim-{i}",
                    "content": f"This is a simulated Discord message #{i} for error recovery.",
                    "user_id": "123456789",
                    "timestamp": datetime.now().isoformat(),
                    "channel_id": str(channel_id),
                    "guild_id": "987654321"
                } for i in range(1, 4)
            ]
            for msg in simulated_messages:
                logger.info(f"DRY-RUN: Collected from Discord: {msg['content']}")
            return simulated_messages
        return []

def collect_x_mentions(limit=10, dry_run=True):
    """Collect recent mentions/replies from X
    
    Args:
        limit (int): Maximum number of mentions to collect
        dry_run (bool): If True, just log without storing
        
    Returns:
        list: Collected mentions
    """
    if not x_client:
        logger.error("X client not initialized")
        return []
    
    # Load processed IDs
    processed_data = load_processed_ids(PROCESSED_X_IDS_FILE, {"ids": []})
    processed_ids = set(processed_data["ids"])
    
    try:
        # Get mentions timeline
        mentions = x_client.mentions_timeline(count=limit)
        
        collected = []
        for tweet in mentions:
            # Skip already processed tweets
            if str(tweet.id) in processed_ids:
                continue
            
            collected.append({
                "tweet_id": str(tweet.id),
                "content": tweet.text,
                "user_id": str(tweet.user.id),
                "user_screen_name": tweet.user.screen_name,
                "timestamp": tweet.created_at.isoformat() if hasattr(tweet, "created_at") else datetime.now().isoformat()
            })
            
            # Add to processed IDs
            processed_ids.add(str(tweet.id))
            
            if dry_run:
                logger.info(f"DRY-RUN: Collected from X: {tweet.text[:100]}{'...' if len(tweet.text) > 100 else ''}")
        
        # Update processed IDs file
        processed_data["ids"] = list(processed_ids)
        save_processed_ids(processed_data, PROCESSED_X_IDS_FILE)
        
        logger.info(f"Collected {len(collected)} new X mentions")
        return collected
    except Exception as e:
        logger.error(f"Error collecting X mentions: {e}")
        if "403 Forbidden" in str(e) and "different access level" in str(e):
            logger.warning("X API access level insufficient - requires Elevated access for mentions_timeline")
        
        if dry_run:
            # Return simulated data for testing
            logger.info("DRY-RUN: Using simulated X mentions due to API limitations")
            simulated_mentions = [
                {
                    "tweet_id": f"sim-{i}",
                    "content": f"@SpeedoTino This is a simulated X mention #{i} for testing PulseCheck.",
                    "user_id": "987654321",
                    "user_screen_name": f"test_user_{i}",
                    "timestamp": datetime.now().isoformat()
                } for i in range(1, 4)
            ]
            for tweet in simulated_mentions:
                logger.info(f"DRY-RUN: Collected from X: {tweet['content']}")
            return simulated_mentions
        return []

def analyze_and_store_sentiment(messages, platform, dry_run=True):
    """Analyze sentiment of messages and store in database
    
    Args:
        messages (list): List of message dictionaries
        platform (str): Platform identifier (discord, x)
        dry_run (bool): If True, just log without storing
        
    Returns:
        int: Number of messages analyzed
    """
    if not messages:
        return 0
    
    if dry_run:
        logger.info(f"DRY-RUN: Would analyze {len(messages)} {platform} messages")
        return len(messages)
    
    # Import HF transformer for sentiment analysis
    try:
        from transformers import pipeline
    except ImportError:
        logger.error("Failed to import transformers. Try: pip install transformers")
        return 0
    
    try:
        # Load sentiment analysis model
        sentiment_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
        
        conn = get_db_connection()
        if not conn:
            logger.error("Database connection failed")
            return 0
        
        cursor = conn.cursor()
        
        for msg in messages:
            content = msg.get("content", "")
            if not content:
                continue
            
            # Analyze sentiment
            try:
                # Limit input size to avoid model errors
                truncated_content = content[:512]
                result = sentiment_analyzer(truncated_content)
                sentiment = result[0]
                
                # Extract values
                sentiment_label = sentiment["label"]
                confidence = sentiment["score"]
                
                # Extract topics
                topics = extract_topics(content)
                
                # Store in database based on platform
                if platform == "discord":
                    # First insert into discord_messages if not exists
                    cursor.execute("""
                        INSERT INTO discord_messages 
                        (message_id, content, user_id, timestamp, channel_id, guild_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (message_id) DO NOTHING
                    """, (
                        msg["message_id"],
                        content,
                        msg["user_id"],
                        msg["timestamp"],
                        msg.get("channel_id", ""),
                        msg.get("guild_id", "")
                    ))
                    
                    # Then insert sentiment analysis
                    cursor.execute("""
                        INSERT INTO sentiment_analysis
                        (message_id, platform, sentiment, confidence, topics)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (message_id, platform) DO UPDATE
                        SET sentiment = EXCLUDED.sentiment,
                            confidence = EXCLUDED.confidence,
                            topics = EXCLUDED.topics
                    """, (
                        msg["message_id"],
                        platform,
                        sentiment_label,
                        confidence,
                        topics
                    ))
                
                elif platform == "x":
                    # For X, we store it differently since there's no dedicated table
                    # We'll use a JSON field to store the metadata
                    cursor.execute("""
                        INSERT INTO sentiment_analysis
                        (message_id, platform, sentiment, confidence, topics, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (message_id, platform) DO UPDATE
                        SET sentiment = EXCLUDED.sentiment,
                            confidence = EXCLUDED.confidence,
                            topics = EXCLUDED.topics,
                            metadata = EXCLUDED.metadata
                    """, (
                        msg["tweet_id"],
                        platform,
                        sentiment_label,
                        confidence,
                        topics,
                        json.dumps({
                            "content": content,
                            "user_id": msg["user_id"],
                            "user_screen_name": msg.get("user_screen_name", ""),
                            "timestamp": msg.get("timestamp", datetime.now().isoformat())
                        })
                    ))
                
                logger.info(f"Analyzed {platform} message: {sentiment_label} ({confidence:.2f})")
            
            except Exception as e:
                logger.error(f"Error analyzing {platform} message {msg.get('message_id', 'unknown')}: {e}")
                continue
        
        conn.commit()
        logger.info(f"Stored sentiment for {len(messages)} {platform} messages")
        return len(messages)
    
    except Exception as e:
        logger.error(f"Error in sentiment analysis and storage: {e}")
        if conn:
            conn.rollback()
        return 0
    finally:
        if conn:
            cursor.close()
            conn.close()

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
        # Ensure topics column exists
        ensure_topics_column_exists()
        
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
                        x_client.update_status(status=message)
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

async def run_live_collection(dry_run=True, duration_minutes=30, discord_channel_id=None, interval_minutes=15):
    """Run live collection from X and Discord
    
    Args:
        dry_run (bool): If True, just log without posting
        duration_minutes (int): How long to run collection for
        discord_channel_id (int): ID of Discord channel to collect from
        interval_minutes (int): Minutes between collection cycles
    """
    # Authenticate with platforms
    platforms = authenticate_platforms(['x', 'discord'], dry_run)
    
    if not any(platforms.values()):
        logger.error("No platforms available for collection. Exiting.")
        return False
    
    logger.info(f"Starting live collection for {duration_minutes} minutes (dry_run={dry_run}, interval={interval_minutes} minutes)")
    
    start_time = datetime.now()
    end_time = start_time + timedelta(minutes=duration_minutes)
    
    # Set up Discord client event
    if platforms["discord"] and discord_channel_id:
        @discord_client.event
        async def on_ready():
            logger.info(f"Connected to Discord as DeepThought#8885")
            logger.info(f"Monitoring channel {discord_channel_id} in server 348595593800843280")
        
        # Start Discord client - try to login but don't block if it fails
        discord_task = None
        try:
            if dry_run:
                logger.info("DRY-RUN: Skipping actual Discord client login, using simulated data")
            else:
                discord_task = asyncio.create_task(discord_client.start(DISCORD_TOKEN))
        except Exception as e:
            logger.error(f"Error starting Discord client: {e}")
    else:
        discord_task = None
    
    try:
        iteration = 0
        while datetime.now() < end_time:
            iteration += 1
            logger.info(f"Collection iteration {iteration} started")
            
            # Collect from X (every interval_minutes, Twitter rate limits)
            x_count = 0
            if platforms["x"]:
                logger.info("Collecting from X mentions...")
                x_mentions = collect_x_mentions(limit=10, dry_run=dry_run)
                if x_mentions:
                    x_count = analyze_and_store_sentiment(x_mentions, "x", dry_run=dry_run)
            
            # Collect from Discord
            discord_count = 0
            if platforms["discord"] and discord_channel_id:
                logger.info(f"Collecting from Discord channel {discord_channel_id}...")
                discord_messages = await collect_discord_messages(discord_channel_id, limit=10, dry_run=dry_run)
                if discord_messages:
                    discord_count = analyze_and_store_sentiment(discord_messages, "discord", dry_run=dry_run)
            
            # Log cycle summary
            logger.info(f"Cycle {iteration}: Collected {x_count} X mentions, {discord_count} Discord messages")
            
            # Calculate time remaining and sleep appropriately
            now = datetime.now()
            if now >= end_time:
                break
                
            remaining = (end_time - now).total_seconds()
            # For testing purposes, use shorter sleep intervals
            sleep_time = min(60 * 1 if iteration < 3 else 60 * interval_minutes, remaining)  # First few iterations quicker
            
            logger.info(f"Waiting {sleep_time:.1f} seconds before next collection...")
            await asyncio.sleep(sleep_time)
    
    except KeyboardInterrupt:
        logger.info("Live collection interrupted by user")
    except Exception as e:
        logger.error(f"Error in live collection: {e}")
    finally:
        # Clean up Discord client
        if discord_task and not discord_task.done():
            discord_task.cancel()
        
        logger.info("Live collection completed")
    
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='PulseCheck - Collect and post sentiment insights')
    parser.add_argument('--dry-run', action='store_true', help='Log actions without posting or storing')
    parser.add_argument('--platform', choices=['x', 'bluesky', 'discord', 'both'], default='both', 
                        help='Platform to use (default: both)')
    parser.add_argument('--count', type=int, default=5, 
                        help='Number of posts per platform (default: 5)')
    parser.add_argument('--live', action='store_true', 
                        help='Enable live collection mode')
    parser.add_argument('--duration', type=int, default=30,
                        help='Duration in minutes for live collection (default: 30)')
    parser.add_argument('--discord-channel', type=int,
                        help='Discord channel ID for live collection')
    parser.add_argument('--interval', type=int, default=15,
                        help='Minutes between collection cycles (default: 15)')
    args = parser.parse_args()
    
    # Convert platform argument to a list
    if args.platform == 'both':
        target_platforms = None
    else:
        target_platforms = [args.platform]
    
    success = False
    if args.live:
        # Run in live collection mode
        if not args.discord_channel:
            logger.error("Discord channel ID is required for live collection")
            sys.exit(1)
        
        success = asyncio.run(run_live_collection(
            dry_run=args.dry_run,
            duration_minutes=args.duration,
            discord_channel_id=args.discord_channel,
            interval_minutes=args.interval
        ))
    else:
        # Run in regular posting mode
        success = post_sentiment_summary(
            platform_limit=args.count, 
            dry_run=args.dry_run, 
            target_platforms=target_platforms
        )
        
    sys.exit(0 if success else 1) 