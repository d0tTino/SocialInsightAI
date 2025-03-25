import asyncio
import logging
from sentiment_bot import run_live_collection

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Discord channel ID for a public channel
DISCORD_CHANNEL_ID = 691490703754154034  # Python Discord - #python-help

async def main():
    """Run live collection for a short time"""
    logger.info("Starting test live collection")
    
    # Run for 2 minutes in dry-run mode
    await run_live_collection(
        dry_run=True,
        duration_minutes=2,
        discord_channel_id=DISCORD_CHANNEL_ID
    )
    
    logger.info("Test live collection completed")

if __name__ == "__main__":
    asyncio.run(main()) 