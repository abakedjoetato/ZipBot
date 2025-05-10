#!/usr/bin/env python3
"""
Discord Bot CSV Testing Script

This script tests the Discord bot's ability to process CSV files.
"""

import os
import sys
import logging
import asyncio
import discord
from discord.ext import commands
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('discord_test.log')
    ]
)

logger = logging.getLogger('discord_test')

# Bot configuration
TOKEN = os.environ.get('DISCORD_BOT_TOKEN')
if not TOKEN:
    logger.error("No Discord bot token found in environment variables!")
    sys.exit(1)

# Create bot instance with all intents
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Store test results
test_results = {
    "csv_files_found": 0,
    "csv_files_parsed": 0,
    "events_processed": 0,
    "test_channel_id": None,
    "test_guild_id": None,
    "errors": []
}

@bot.event
async def on_ready():
    """Called when the bot is ready"""
    logger.info(f'Logged in as {bot.user.name} ({bot.user.id})')
    logger.info('------')
    
    # Find a suitable test channel
    for guild in bot.guilds:
        logger.info(f"Connected to guild: {guild.name} (ID: {guild.id})")
        test_results["test_guild_id"] = guild.id
        
        # Look for a text channel
        for channel in guild.text_channels:
            logger.info(f"Found channel: {channel.name} (ID: {channel.id})")
            if test_results["test_channel_id"] is None:
                test_results["test_channel_id"] = channel.id
                logger.info(f"Selected channel for testing: {channel.name}")
    
    if test_results["test_channel_id"]:
        # Run the tests
        await run_tests()
    else:
        logger.error("No suitable test channel found! Cannot proceed with tests.")
        await bot.close()

async def run_tests():
    """Run CSV processing tests"""
    channel = bot.get_channel(test_results["test_channel_id"])
    if not channel:
        logger.error(f"Could not find channel with ID {test_results['test_channel_id']}")
        await bot.close()
        return
    
    try:
        # Test 1: List CSV files
        logger.info("TEST 1: Listing available CSV files")
        await channel.send("!csv list")
        await asyncio.sleep(5)  # Wait for response
        
        # Test 2: Process a specific CSV file
        logger.info("TEST 2: Processing a specific CSV file (attached_assets/2025.05.09-11.58.37.csv)")
        await channel.send("!csv process attached_assets/2025.05.09-11.58.37.csv")
        await asyncio.sleep(10)  # Wait for processing
        
        # Test 3: Process all CSV files
        logger.info("TEST 3: Processing all CSV files")
        await channel.send("!csv process all")
        await asyncio.sleep(30)  # Wait longer for all files processing
        
        # Test 4: View statistics from processed data
        logger.info("TEST 4: Viewing statistics from processed data")
        await channel.send("!stats top weapons")
        await asyncio.sleep(5)  # Wait for response
        
        # Test 5: Check the most recent events
        logger.info("TEST 5: Checking recent events")
        await channel.send("!events recent 5")
        await asyncio.sleep(5)  # Wait for response
        
        # Conclude tests
        logger.info("All tests completed.")
        await channel.send("CSV processing tests completed. Check the logs for results.")
        
        # After tests, close the bot
        await bot.close()
        
    except Exception as e:
        logger.error(f"Error during tests: {str(e)}")
        import traceback
        traceback.print_exc()
        await bot.close()

@bot.event
async def on_message(message):
    """Override to capture bot responses for test validation"""
    # Don't respond to our own messages
    if message.author == bot.user:
        # Log the bot's responses for test validation
        content = message.content
        if len(content) > 100:
            content = content[:100] + "..."
        logger.info(f"Bot response: {content}")
        
        # Look for specific patterns in responses to validate tests
        if "CSV files found" in message.content:
            # Extract the number of files found
            match = re.search(r"(\d+) CSV files found", message.content)
            if match:
                test_results["csv_files_found"] = int(match.group(1))
                logger.info(f"CSV files found: {test_results['csv_files_found']}")
        
        elif "Processed" in message.content and "events" in message.content:
            # Extract the number of events processed
            match = re.search(r"Processed (\d+) events", message.content)
            if match:
                test_results["events_processed"] += int(match.group(1))
                test_results["csv_files_parsed"] += 1
                logger.info(f"Total events processed: {test_results['events_processed']}")
                logger.info(f"CSV files parsed: {test_results['csv_files_parsed']}")
    
    # Process commands
    await bot.process_commands(message)

def main():
    """Main entry point"""
    try:
        bot.run(TOKEN)
    except Exception as e:
        logger.error(f"Error running bot: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # Log test summary
    logger.info("\n" + "="*50)
    logger.info("TEST SUMMARY")
    logger.info("="*50)
    logger.info(f"CSV files found: {test_results['csv_files_found']}")
    logger.info(f"CSV files parsed: {test_results['csv_files_parsed']}")
    logger.info(f"Total events processed: {test_results['events_processed']}")
    logger.info(f"Test guild ID: {test_results['test_guild_id']}")
    logger.info(f"Test channel ID: {test_results['test_channel_id']}")
    
    if test_results["errors"]:
        logger.info("Errors encountered:")
        for error in test_results["errors"]:
            logger.info(f"- {error}")
    else:
        logger.info("No errors encountered during testing.")
    
    # Report success/failure
    if test_results["csv_files_found"] > 0 and test_results["csv_files_parsed"] > 0:
        logger.info("TEST PASSED: CSV files were found and parsed successfully.")
        return 0
    else:
        logger.error("TEST FAILED: CSV files were not properly processed.")
        return 1

if __name__ == "__main__":
    import re  # For pattern matching in responses
    sys.exit(main())