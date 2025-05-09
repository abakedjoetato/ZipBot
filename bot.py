"""
Bot configuration and setup
"""
import os
import logging
import discord
from discord.ext import commands
import config

logger = logging.getLogger(__name__)

def setup_bot():
    """
    Configure and set up the Discord bot with all necessary configurations and cogs
    """
    # Load bot configuration
    bot_config = config.load_config()
    
    # Set up the bot with appropriate intents
    intents = discord.Intents.default()
    intents.message_content = True  # Enable message content intent for commands
    
    bot = commands.Bot(
        command_prefix=bot_config.get("prefix", "!"),
        intents=intents,
        description=bot_config.get("description", "Discord Bot")
    )
    
    # Event: Bot is ready
    @bot.event
    async def on_ready():
        logger.info(f"Logged in as {bot.user.name} (ID: {bot.user.id})")
        logger.info(f"Connected to {len(bot.guilds)} guilds")
        logger.info("------")
        
        # Set the bot's activity/presence if configured
        if "activity" in bot_config:
            activity_type = bot_config.get("activity_type", "playing")
            activity_name = bot_config.get("activity", "with Discord")
            
            if activity_type.lower() == "playing":
                activity = discord.Game(name=activity_name)
            elif activity_type.lower() == "watching":
                activity = discord.Activity(type=discord.ActivityType.watching, name=activity_name)
            elif activity_type.lower() == "listening":
                activity = discord.Activity(type=discord.ActivityType.listening, name=activity_name)
            else:
                activity = discord.Game(name=activity_name)
                
            await bot.change_presence(activity=activity)
    
    # Load cogs (extension modules)
    load_cogs(bot)
    
    return bot

def load_cogs(bot):
    """Load all available cogs from the cogs directory"""
    # Get the list of available cogs
    cogs_dir = os.path.join(os.path.dirname(__file__), "cogs")
    
    if not os.path.exists(cogs_dir):
        logger.warning(f"Cogs directory not found: {cogs_dir}")
        return
    
    # Load each cog
    for filename in os.listdir(cogs_dir):
        if filename.endswith(".py") and not filename.startswith("_"):
            cog_name = f"cogs.{filename[:-3]}"
            try:
                bot.load_extension(cog_name)
                logger.info(f"Loaded extension: {cog_name}")
            except Exception as e:
                logger.error(f"Failed to load extension {cog_name}: {e}")
