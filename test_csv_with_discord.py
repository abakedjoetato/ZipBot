#!/usr/bin/env python3
"""
Discord Bot CSV Testing Script

This script tests the CSV processing capabilities through the Discord bot directly.
It runs a minimal version of the Discord bot and processes test CSV files.
"""

import os
import sys
import logging
import asyncio
import discord
from discord.ext import commands

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('discord_csv_test')

# Import necessary modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.csv_parser import CSVParser
from cogs.csv_processor import CSVProcessor

# Bot configuration
TOKEN = os.environ.get('DISCORD_BOT_TOKEN')
TEST_CHANNEL_ID = None  # Will be set based on available channels

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

@bot.event
async def on_ready():
    """Called when the bot is ready to receive events"""
    logger.info(f'Logged in as {bot.user.name} ({bot.user.id})')
    
    # Find a suitable channel for testing
    global TEST_CHANNEL_ID
    for guild in bot.guilds:
        logger.info(f"Connected to guild: {guild.name} (ID: {guild.id})")
        for channel in guild.text_channels:
            logger.info(f"Available channel: {channel.name} (ID: {channel.id})")
            # Use the first text channel for testing
            if TEST_CHANNEL_ID is None:
                TEST_CHANNEL_ID = channel.id
                logger.info(f"Selected channel for testing: {channel.name} (ID: {channel.id})")
    
    # Add the CSV processor cog
    await bot.add_cog(CSVProcessor(bot))
    
    # Start the test sequence
    await run_tests()

async def run_tests():
    """Run the CSV processing tests"""
    logger.info("Starting CSV parsing tests...")
    
    # Make sure we have a test channel
    if not TEST_CHANNEL_ID:
        logger.error("No suitable test channel found. Cannot continue tests.")
        await bot.close()
        return
    
    channel = bot.get_channel(TEST_CHANNEL_ID)
    if not channel:
        logger.error(f"Could not find channel with ID {TEST_CHANNEL_ID}")
        await bot.close()
        return
    
    logger.info(f"Using channel {channel.name} for tests")
    
    try:
        # Test 1: List available CSV files
        logger.info("Test 1: Listing available CSV files")
        await channel.send("!csv list")
        await asyncio.sleep(5)  # Wait for response
        
        # Test 2: Process a specific CSV file
        logger.info("Test 2: Processing a specific CSV file")
        await channel.send("!csv process attached_assets/2025.05.09-11.58.37.csv")
        await asyncio.sleep(5)  # Wait for response
        
        # Test 3: Process all CSV files
        logger.info("Test 3: Processing all CSV files")
        await channel.send("!csv process all")
        await asyncio.sleep(10)  # Wait for response
        
        # Report results
        logger.info("All tests completed. Check the logs for results.")
        
        # Close the bot connection after tests
        await bot.close()
        
    except Exception as e:
        logger.error(f"Error during tests: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await bot.close()

# Run the bot with the token
def main():
    """Main entry point"""
    if not TOKEN:
        logger.error("No Discord bot token found in environment variables.")
        sys.exit(1)
    
    try:
        bot.run(TOKEN)
    except discord.errors.LoginFailure:
        logger.error("Invalid Discord bot token.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error running bot: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()