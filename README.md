# SocialInsightAI (PulseCheck)

A powerful social media sentiment analysis platform that processes messages from Discord and Bluesky, performs sentiment analysis, and shares insights through automated reporting and social media posting.

## Project Components

### Data Collection and Storage
- Discord and Bluesky data collection modules
- **NEW: Live collection from X mentions and Discord channels**
- PostgreSQL database integration
- Transaction management and data integrity protection

### Sentiment Analysis
- `analyze_sentiment.py`: Processes messages from Discord and Bluesky using the Hugging Face transformers library
- Topic extraction using keyword extraction
- Robust progress tracking and error handling

### Reporting and Sharing
- `sentiment_bot.py`: Posts positive sentiment insights to X and Bluesky
- `report_sentiment.py`: Generates reports of top positive posts
- Dry-run capability for testing without live posting

## Features
- Multi-platform support (Discord, Bluesky, X)
- **NEW: Live data collection with rate limiting and error handling**
- Sentiment classification with confidence scoring
- Topic tagging for contextual insights
- Progress tracking and resume capability
- Detailed logging
- Configurable post limits and time windows

## How to Use

### Setup
1. Ensure PostgreSQL is installed and running
2. Update credentials in `config.py` with your API keys
3. Install dependencies: `pip install tweepy atproto psycopg2 transformers nltk discord.py`

### Running Sentiment Analysis
```
python analyze_sentiment.py
```

### Generating Reports
```
python report_sentiment.py
```
This creates a `sentiment_report.txt` file with the top positive posts.

### Posting to Social Media
For testing without posting:
```
python sentiment_bot.py --dry-run
```

For live posting:
```
python sentiment_bot.py
```

### Live Data Collection
For collecting live data from X and Discord:
```
python sentiment_bot.py --live --dry-run --discord-channel CHANNEL_ID --duration 30
```

Options:
- `--platform x,bluesky,discord`: Specify which platforms to use
- `--count 10`: Set the maximum number of posts per platform
- `--days 30`: Set the time window for post selection
- `--live`: Enable live collection mode
- `--discord-channel`: Specify Discord channel ID for collection
- `--duration`: Set duration in minutes for live collection

## Project Structure
- `analyze_sentiment.py`: Main sentiment analysis engine
- `sentiment_bot.py`: Social media posting and live collection functionality
- `report_sentiment.py`: Reporting tool
- `test_live_collection.py`: Test script for live collection
- `config.py`: Configuration and credentials
- `processed_ids.txt`: Tracking file for processed messages
- `processed_x_ids.json`: Tracking file for processed X mentions
- `processed_discord_ids.json`: Tracking file for processed Discord messages
- `sentiment_report.txt`: Output report file
