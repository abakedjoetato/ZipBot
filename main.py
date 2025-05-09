#!/usr/bin/env python3
"""
Main entry point for Discord bot
"""
import os
import logging
from bot import setup_bot

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run the bot
    bot = setup_bot()
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        logging.error("No Discord token found. Please set the DISCORD_TOKEN environment variable.")
        exit(1)
    
    bot.run(token)
