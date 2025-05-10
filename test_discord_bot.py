#!/usr/bin/env python3
"""
Discord Bot Integration Test with CSV Processing

This script tests the Discord bot integration with the CSV processor
to verify that all CSV files are properly found, parsed, and processed.
"""
import os
import logging
import asyncio
import sys
import json
from datetime import datetime

try:
    import discord
    from discord.ext import commands
except ImportError:
    print("Discord.py not found. Installing required packages...")
    import subprocess
    subprocess.run(["pip", "install", "discord.py"], check=True)
    import discord
    from discord.ext import commands

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('discord_test.log')
    ]
)
logger = logging.getLogger('discord_test')

class TestResults:
    """Store test results for reporting"""
    def __init__(self):
        self.csv_files_found = 0
        self.csv_files_processed = 0
        self.events_processed = 0
        self.errors = []
        self.start_time = datetime.now()
        
    def add_error(self, error):
        """Add an error to the results"""
        self.errors.append(error)
        
    def to_dict(self):
        """Convert results to dictionary"""
        return {
            "csv_files_found": self.csv_files_found,
            "csv_files_processed": self.csv_files_processed,
            "events_processed": self.events_processed,
            "errors": self.errors,
            "start_time": self.start_time.isoformat(),
            "end_time": datetime.now().isoformat(),
            "duration_seconds": (datetime.now() - self.start_time).total_seconds(),
            "success": len(self.errors) == 0 and self.csv_files_processed > 0
        }
        
    def save_to_file(self, filename="csv_test_results.json"):
        """Save results to JSON file"""
        with open(filename, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
            
    def print_summary(self):
        """Print a summary of the test results"""
        success = len(self.errors) == 0 and self.csv_files_processed > 0
        print("\n========================================")
        print(f"CSV Processing Test Results: {'SUCCESS' if success else 'FAILURE'}")
        print("========================================")
        print(f"CSV Files Found:     {self.csv_files_found}")
        print(f"CSV Files Processed: {self.csv_files_processed}")
        print(f"Events Processed:    {self.events_processed}")
        print(f"Errors:              {len(self.errors)}")
        if self.errors:
            print("\nErrors encountered:")
            for i, error in enumerate(self.errors, 1):
                print(f"  {i}. {error}")
        print("========================================")

async def test_csv_processor(bot, channel_id):
    """Test the CSV processor with the actual Discord bot"""
    logger.info("Starting CSV processor test")
    
    results = TestResults()
    
    try:
        # Test direct CSV processing
        from utils.csv_parser import CSVParser
        from utils.sftp import SFTPClient
        
        # Find CSV files in attached_assets directory
        csv_files = [f for f in os.listdir('attached_assets') if f.endswith('.csv')]
        results.csv_files_found = len(csv_files)
        logger.info(f"Found {len(csv_files)} CSV files in attached_assets directory")
        
        # Process each CSV file
        csv_parser = CSVParser()
        for csv_file in csv_files:
            file_path = os.path.join('attached_assets', csv_file)
            logger.info(f"Processing {file_path}")
            
            try:
                with open(file_path, 'r') as f:
                    csv_data = f.read()
                
                events = csv_parser.parse_csv_data(csv_data, file_path)
                logger.info(f"Parsed {len(events)} events from {file_path}")
                results.csv_files_processed += 1
                results.events_processed += len(events)
                
                # Log sample events for verification
                if events:
                    logger.info(f"Sample event: {events[0]}")
            except Exception as e:
                error_msg = f"Error processing {file_path}: {str(e)}"
                logger.error(error_msg)
                results.add_error(error_msg)
        
        # Test bot integration if channel_id is provided
        if channel_id:
            channel = bot.get_channel(int(channel_id))
            if not channel:
                error_msg = f"Channel with ID {channel_id} not found"
                logger.error(error_msg)
                results.add_error(error_msg)
            else:
                logger.info(f"Found channel: {channel.name} ({channel.id})")
                
                # Send test message to channel
                await channel.send(f"CSV Processing Test: Found {results.csv_files_found} files, processed {results.csv_files_processed} files with {results.events_processed} events")
                logger.info("Test message sent to channel")
    
    except Exception as e:
        error_msg = f"Unexpected error in test: {str(e)}"
        logger.error(error_msg)
        results.add_error(error_msg)
    
    # Save and print results
    results.save_to_file()
    results.print_summary()
    return results

async def run_tests():
    """Run the Discord bot tests"""
    # Check bot token
    bot_token = os.environ.get('DISCORD_BOT_TOKEN')
    if not bot_token:
        logger.error("DISCORD_BOT_TOKEN environment variable not set")
        sys.exit(1)
    
    # Initialize bot
    intents = discord.Intents.default()
    intents.message_content = True
    bot = commands.Bot(command_prefix='!', intents=intents)
    
    @bot.event
    async def on_ready():
        """Called when the bot is ready"""
        logger.info(f"Bot connected as {bot.user.name} ({bot.user.id})")
        
        # Get channel ID from environment or use a default test channel
        channel_id = os.environ.get('TEST_CHANNEL_ID')
        if not channel_id:
            # Find a suitable channel for testing
            for guild in bot.guilds:
                for channel in guild.text_channels:
                    if channel.permissions_for(guild.me).send_messages:
                        channel_id = channel.id
                        logger.info(f"Using channel {channel.name} ({channel_id}) for testing")
                        break
                if channel_id:
                    break
        
        # Run tests
        if channel_id:
            await test_csv_processor(bot, channel_id)
        else:
            logger.warning("No suitable channel found for testing")
            # Still run CSV tests without Discord integration
            await test_csv_processor(bot, None)
        
        # Close the bot after testing
        await bot.close()
    
    try:
        # Start the bot
        await bot.start(bot_token)
    except discord.errors.LoginFailure:
        logger.error("Invalid bot token")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error starting bot: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(run_tests())