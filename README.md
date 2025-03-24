# SocialInsightAI

A sentiment analysis and social media posting application that analyzes messages from Discord and Bluesky, then posts insights about positive sentiment to social platforms.

## Components

### 1. Data Import
- `import_discord_data.py`: Imports Discord message data from JSON files into PostgreSQL
- Data is sourced from the bot DeepThought#8885

### 2. Sentiment Analysis
- `analyze_sentiment.py`: Analyzes message content from Discord and Bluesky using Hugging Face's sentiment analysis model
- Processes data in batches for efficiency
- Stores sentiment results (POSITIVE/NEGATIVE) and confidence scores in the database

### 3. Sentiment Bot
- `sentiment_bot.py`: Posts positive sentiment insights to social media platforms (X and Bluesky)
- Includes a dry-run mode for testing without posting (`--dry-run` flag)
- Handles platform authentication and database connectivity errors gracefully

## Database Structure

- `discord_messages`: Stores Discord message data
- `bluesky_posts`: Stores Bluesky post data
- `sentiment_analysis`: Stores sentiment analysis results

## Setup and Usage

1. Configure credentials in `config.py`:
   - Discord token
   - Bluesky username and password
   - X (Twitter) API credentials
   - PostgreSQL database credentials

2. Import data:
   ```
   python import_discord_data.py
   ```

3. Analyze sentiment:
   ```
   python analyze_sentiment.py
   ```

4. Post sentiment insights (dry-run mode):
   ```
   python sentiment_bot.py --dry-run
   ```

5. Post sentiment insights (live mode):
   ```
   python sentiment_bot.py
   ```

## Features

- Batch processing for efficiency
- Error handling and logging
- Transaction support for database operations
- Progress tracking for long-running operations
- Dry-run mode for testing without posting to social platforms
- Filters for high-confidence positive sentiment posts

## Future Enhancements

- Additional social media platform integrations
- More advanced sentiment analysis models
- Topic categorization
- User interface for monitoring and configuration
