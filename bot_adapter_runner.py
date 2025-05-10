#!/usr/bin/env python3
"""
Adapter script for running the Discord bot with either py-cord or discord.py

This script automatically detects the available Discord library 
and provides the necessary compatibility for the bot to run.
"""
import os
import sys
import logging
import asyncio
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('adapter_bot.log')
    ]
)
logger = logging.getLogger('bot_adapter')

# Import our adapter which handles library compatibility
try:
    import pycord_adapter
    logger.info(f"Using Discord library: {pycord_adapter.get_library_info()}")
except ImportError:
    logger.critical("Failed to import pycord_adapter! This is required to run the bot.")
    sys.exit(1)

async def run_bot_with_adapter():
    """Run the Discord bot with our adapter"""
    # Print banner
    print("=" * 60)
    print("  Emeralds Killfeed PvP Statistics Discord Bot")
    print("  Using Discord Library Adapter")
    print(f"  {pycord_adapter.get_library_info()}")
    print("=" * 60)
    
    # Import the main bot module
    try:
        from bot import main as bot_main
        result = bot_main()
        return result
    except Exception as e:
        logger.error(f"Error running bot: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    # Run the bot with our adapter
    try:
        exit_code = asyncio.run(run_bot_with_adapter())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
        sys.exit(1)