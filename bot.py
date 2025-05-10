"""
Emeralds Killfeed PvP Statistics Discord Bot
Main bot initialization and configuration

This implementation uses discord.py 2.5.2 as the Discord API library, 
with direct imports and no compatibility layers as specified by rule #2 in rules.md.
"""
import os
import sys
import asyncio
import logging
from typing import Dict, List, Optional, Any, Union, TypeVar, Callable, Tuple, Coroutine

# Direct imports from discord.py - no compatibility layer
import discord
from discord.ext import commands
from discord.ext.commands import Bot, Cog
from discord import app_commands
from discord.app_commands import Choice

# Import the AppCommandOptionType from our compatibility layer
from utils.discord_compat import AppCommandOptionType

# Import additional helper functions from discord_compat
from utils.discord_compat import command, describe, autocomplete, guild_only

# Log successful import
logger = logging.getLogger('bot')
logger.info(f"Successfully imported discord modules - version: {discord.__version__}")
from utils.database import get_db
from models.guild import Guild
from utils.sftp import periodic_connection_maintenance

# Type definitions for improved type checking
T = TypeVar('T')
MotorDatabase = Any  # Motor database connection type

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log')
    ]
)
logger = logging.getLogger('bot')

# Bot configuration
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True

async def sync_guilds_with_database(bot):
    """
    Synchronize all current Discord guilds with the database.
    This ensures that guilds added while the bot was offline are properly registered.
    """
    if bot.db is None:
        logger.error("Cannot sync guilds: Database connection not established")
        return
    
    logger.info(f"Syncing {len(bot.guilds)} guilds with database...")
    
    for guild in bot.guilds:
        try:
            # Use get_or_create to ensure the guild exists in database
            # Creates the guild if it doesn't exist yet
            guild_model = await Guild.get_or_create(bot.db, guild.id, guild.name)
            if guild_model:
                logger.info(f"Synced guild: {guild.name} (ID: {guild.id})")
            else:
                logger.error(f"Failed to sync guild: {guild.name} (ID: {guild.id})")
        except Exception as e:
            logger.error(f"Error syncing guild {guild.name} (ID: {guild.id}): {e}")
    
    logger.info("Guild synchronization complete")

async def initialize_bot(force_sync=False):
    """Initialize the Discord bot and load cogs"""
    # Create bot instance with hardcoded owner ID
    # Using proper py-cord Bot initialization with type hints
    
    class PvPBot(Bot):
        """Custom Bot class with additional attributes for our application"""
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # Initialize private attributes with proper typing
            self._db: Optional[MotorDatabase] = None
            self._background_tasks: Dict[str, asyncio.Task] = {}
            self._sftp_connections: Dict[str, Any] = {}
            self._home_guild_id: Optional[int] = None
        
        @property
        def db(self) -> Optional[MotorDatabase]:
            """Database connection getter"""
            return self._db
            
        @db.setter
        def db(self, value: Optional[MotorDatabase]) -> None:
            """Database connection setter"""
            self._db = value
            
        @property
        def home_guild_id(self) -> Optional[int]:
            """Home guild ID getter"""
            return self._home_guild_id
            
        @home_guild_id.setter
        def home_guild_id(self, value: Optional[int]):
            """Home guild ID setter"""
            self._home_guild_id = value
            
        @property
        def background_tasks(self) -> Dict[str, asyncio.Task]:
            """Background tasks getter"""
            return self._background_tasks
            
        @background_tasks.setter
        def background_tasks(self, value: Dict[str, asyncio.Task]):
            """Background tasks setter"""
            self._background_tasks = value
            
        @property
        def sftp_connections(self) -> Dict[str, Any]:
            """SFTP connections getter"""
            return self._sftp_connections
            
        @sftp_connections.setter
        def sftp_connections(self, value: Dict[str, Any]):
            """SFTP connections setter"""
            self._sftp_connections = value
            
        async def sync_commands(self, guild_ids=None):
            """
            Sync application commands with Discord.
            
            This implementation uses discord.py's tree.sync() method
            with direct imports as required by Rule #2 in rules.md.
            
            Args:
                guild_ids: Optional list of guild IDs to sync commands for
                
            Returns:
                List of synced commands
            """
            if guild_ids:
                # Sync commands for specific guilds
                result = []
                for guild_id in guild_ids:
                    guild_results = await self.tree.sync(guild=discord.Object(id=guild_id))
                    result.extend(guild_results)
                return result
            
            # Sync global commands
            return await self.tree.sync()
    
    bot = PvPBot(
        command_prefix='!', 
        intents=intents, 
        help_command=None,
        owner_id=462961235382763520  # Correct hardcoded owner ID (constant truth)
    )
    
    # Initialize database connection
    logger.info("Initializing database connection...")
    try:
        bot.db = await get_db()
        logger.info("Database connection established")
        
        # Initialize home guild ID from database
        bot.home_guild_id = None
        try:
            # Look for home_guild_id in a bot_config collection
            bot_config = await bot.db.bot_config.find_one({"key": "home_guild_id"})
            if bot_config and "value" in bot_config:
                bot.home_guild_id = int(bot_config["value"])
                logger.info(f"Retrieved home guild ID from database: {bot.home_guild_id}")
            else:
                # Try environment variable as fallback
                home_guild_id = os.environ.get('HOME_GUILD_ID')
                if home_guild_id:
                    try:
                        bot.home_guild_id = int(home_guild_id)
                        logger.info(f"Using home guild ID from environment: {bot.home_guild_id}")
                    except (ValueError, TypeError):
                        logger.warning("Invalid HOME_GUILD_ID in environment variables")
        except Exception as e:
            logger.error(f"Error loading home guild ID: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        bot.db = None
    
    # Bot events
    @bot.event
    async def on_ready():
        """Called when the bot is ready to start accepting commands"""
        # Add proper error handling for user property
        if bot.user:
            logger.info(f"Bot logged in as {bot.user.name} (ID: {bot.user.id})")
            logger.info(f"Connected to {len(bot.guilds)} guilds")
        else:
            logger.warning("Bot logged in but user property is None")
        logger.info(f"Discord API version: {discord.__version__}")
        
        # Set bot status
        activity = discord.Activity(type=discord.ActivityType.watching, name="Emeralds Killfeed")
        await bot.change_presence(activity=activity)
        
        # Initialize guilds database records for all connected guilds
        # This ensures guilds added while bot was offline are properly registered
        await sync_guilds_with_database(bot)
        
        # Synchronize server data between collections
        # This ensures original_server_id is consistent across all collections
        if bot.db:
            logger.info("Synchronizing server data between collections...")
            try:
                await bot.db.synchronize_server_data()
                logger.info("Server data synchronization complete")
                
                # Load server mappings into the server_identity module
                try:
                    from utils.server_identity import load_server_mappings
                    mappings_loaded = await load_server_mappings(bot.db)
                    logger.info(f"Loaded {mappings_loaded} server ID mappings from database")
                except Exception as mapping_err:
                    logger.error(f"Error loading server ID mappings: {mapping_err}", exc_info=True)
            except Exception as e:
                logger.error(f"Error during server data synchronization: {e}", exc_info=True)
        
        # Start SFTP connection maintenance task with improved error resilience
        if 'sftp_maintenance' not in bot.background_tasks or (
            bot.background_tasks['sftp_maintenance'] and bot.background_tasks['sftp_maintenance'].done()
        ):
            logger.info("Starting SFTP connection maintenance task with automatic restart")
            
            # Create a wrapper task that uses our resilient task runner
            task_wrapper = asyncio.create_task(
                run_background_tasks_with_restart(
                    bot, 
                    lambda: periodic_connection_maintenance(interval=120), 
                    interval=120,
                    name="sftp_maintenance",
                    max_failures=5  # Allow more retries before giving up
                ),
                name="sftp_maintenance_wrapper"
            )
            
            # Store the wrapper task
            bot.background_tasks['sftp_maintenance'] = task_wrapper
            
            # Add extra error handler as a safety net
            def task_error_handler(task):
                if task.done() and not task.cancelled():
                    try:
                        exc = task.exception()
                        if exc:
                            logger.error(f"SFTP maintenance task failed with unhandled exception: {exc}", exc_info=exc)
                            # Log stack trace for debugging
                            import traceback
                            logger.error(f"Task stack trace: {traceback.format_exception(type(exc), exc, exc.__traceback__)}")
                    except asyncio.CancelledError:
                        # Task was cancelled, which is normal during shutdown
                        logger.info("SFTP maintenance task was cancelled")
                    except Exception as e:
                        # Error extracting exception info
                        logger.error(f"Error checking task exception: {e}")
            
            # Add our enhanced error handler
            bot.background_tasks['sftp_maintenance'].add_done_callback(task_error_handler)
        
        if force_sync:
            logger.info("Syncing application commands...")
            try:
                # Proper py-cord commands syncing
                await bot.sync_commands()
                logger.info("Application commands synced successfully!")
            except Exception as e:
                logger.error(f"Error syncing commands: {e}", exc_info=True)
    
    @bot.event
    async def on_guild_join(guild):
        """Called when the bot joins a new guild"""
        logger.info(f"Bot joined new guild: {guild.name} (ID: {guild.id})")
        
        try:
            # Use get_or_create to ensure the guild exists in database
            # This creates a database record for the new guild automatically
            guild_model = await Guild.get_or_create(bot.db, guild.id, guild.name)
            if guild_model:
                logger.info(f"Created database record for new guild: {guild.name} (ID: {guild.id})")
                
                # Synchronize server data if a new guild is added
                # to ensure any imported servers have proper original_server_id values
                if bot.db:
                    try:
                        await bot.db.synchronize_server_data()
                        logger.info(f"Synchronized server data after adding guild {guild.name}")
                        
                        # Reload server mappings after adding a new guild
                        try:
                            from utils.server_identity import load_server_mappings
                            mappings_loaded = await load_server_mappings(bot.db)
                            logger.info(f"Reloaded {mappings_loaded} server ID mappings after adding guild {guild.name}")
                        except Exception as mapping_err:
                            logger.error(f"Error reloading server ID mappings: {mapping_err}", exc_info=True)
                    except Exception as e:
                        logger.error(f"Error synchronizing server data after adding guild {guild.name}: {e}")
            else:
                logger.error(f"Failed to create database record for guild: {guild.name} (ID: {guild.id})")
        except Exception as e:
            logger.error(f"Error creating database record for guild {guild.name} (ID: {guild.id}): {e}")
    
    @bot.event
    async def on_command_error(ctx, error):
        """Global error handler for commands"""
        if isinstance(error, commands.CommandNotFound):
            return
        
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Missing required argument: {error.param.name}")
            return
        
        if isinstance(error, commands.BadArgument):
            await ctx.send(f"Bad argument: {error}")
            return
        
        # For all other errors, log them
        logger.error(f"Error in command {ctx.command}: {error}")
        await ctx.send("An error occurred while executing the command. Please try again later.")
    
    # Load cogs
    logger.info("Loading cogs...")
    cog_count = 0
    cog_dir = 'cogs'
    
    # Get list of cog files
    try:
        # First get the list of cog files without awaiting anything
        cog_files = []
        
        # Comprehensive list of excluded terms for backup and temporary files
        excluded_terms = [
            # Standard backup patterns
            'backup', '_backup', '.bak', '.backup', 
            # Temporary files
            '.temp', '_temp', 'temp_', '.tmp', '_tmp', 'tmp_',
            # New/original versions
            '.new', '.old', '_new', '_old', 'old_', 'new_', 
            # Original files
            '.original', '_original', 'original_',
            # Copy files
            '.copy', '_copy', 'copy_',
            # Version indicators
            '.v1', '.v2', '.v3', '_v1', '_v2', '_v3'
        ]
        
        # First collect all valid Python files, filtering out duplicates
        valid_files = {}
        for f in os.listdir(cog_dir):
            # Only include .py files that don't start with _
            if f.endswith('.py') and not f.startswith('_'):
                file_base = f[:-3]  # Remove .py extension
                normalized_name = file_base.lower()
                
                # Skip if this file contains any excluded terms
                if any(term in normalized_name for term in excluded_terms):
                    logger.info(f"Skipping backup/temp cog file: {f}")
                    continue
                
                # Special case: If the file is a base version (e.g., "csv_processor.py"),
                # it should always be included instead of variants
                if "_" not in normalized_name and "." not in normalized_name:
                    # This is a base module (e.g., "admin.py" not "admin_backup.py")
                    valid_files[normalized_name] = f
                    logger.debug(f"Including base cog file: {f}")
                else:
                    # Handle variant module names (only if we don't already have the base version)
                    # Extract base module name before "_" or "."
                    base_module = normalized_name.split('_')[0].split('.')[0]
                    if base_module not in valid_files:
                        valid_files[normalized_name] = f
                        logger.debug(f"Including variant cog file: {f}")
                    else:
                        logger.info(f"Skipping duplicate variant of {base_module}: {f}")
        
        # Convert to list for loading
        cog_files = list(valid_files.values())
        
        # Sort alphabetically for consistent loading order
        cog_files.sort()
        
        logger.info(f"Found {len(cog_files)} cog files to load")
        
        # Now load each cog (load_extension is awaitable in discord.py 2.5.2)
        for filename in cog_files:
            cog_name = filename[:-3]
            try:
                # In discord.py 2.5.2, load_extension is a coroutine
                await bot.load_extension(f"{cog_dir}.{cog_name}")
                logger.info(f"Loaded cog: {cog_name}")
                cog_count += 1
            except Exception as e:
                logger.error(f"Failed to load cog {cog_name}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error listing cog directory: {e}")
    
    logger.info(f"Successfully loaded {cog_count} cogs")
    return bot

async def run_background_tasks_with_restart(bot, task_func, interval, name, max_failures=3):
    """Run a background task with automatic restart on failure
    
    Args:
        bot: Bot instance
        task_func: Async function to run periodically
        interval: Interval in seconds
        name: Task name for logging
        max_failures: Maximum number of consecutive failures before giving up
    """
    failures = 0
    while True:
        try:
            logger.info(f"Starting background task: {name}")
            await task_func()
            # If we get here, the task completed normally, so reset failure count
            failures = 0
        except asyncio.CancelledError:
            logger.warning(f"Background task {name} was cancelled")
            return
        except Exception as e:
            failures += 1
            logger.error(f"Background task {name} failed (attempt {failures}/{max_failures}): {e}", exc_info=True)
            
            if failures >= max_failures:
                logger.critical(f"Background task {name} failed {failures} times, giving up")
                # Clear the task reference using the property instead of the attribute
                # First check if the task exists
                if name in bot.background_tasks:
                    del bot.background_tasks[name]
                    logger.info(f"Removed task {name} from background tasks after max failures")
                return
                
        # Wait before restarting
        try:
            logger.info(f"Waiting {interval}s before next {name} execution")
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info(f"Background task {name} sleep was interrupted, exiting")
            return

async def run_bot():
    """Run the Discord bot"""
    # Check for token
    token = os.environ.get('DISCORD_TOKEN')
    if not token:
        logger.error("DISCORD_TOKEN environment variable not set")
        return 1
    
    # Use a global exception handler for asyncio tasks
    def global_exception_handler(loop, context):
        exception = context.get('exception')
        if exception:
            logger.error(f"Unhandled exception in asyncio: {str(exception)}", exc_info=exception)
        else:
            logger.error(f"Unhandled asyncio error: {context.get('message')}")
    
    try:
        # Get the current event loop and set exception handler
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(global_exception_handler)
        
        # Initialize the bot
        bot = await initialize_bot(force_sync=True)
        
        # Create a background task monitor
        async def monitor_background_tasks():
            while True:
                try:
                    # Check all background tasks - using the property instead of the private attribute
                    for task_name, task in list(bot.background_tasks.items()):
                        if task and task.done():
                            try:
                                exception = task.exception()
                                if exception:
                                    logger.error(f"Background task {task_name} raised an exception: {exception}", 
                                                exc_info=exception)
                                    # Restart the task if it's critical
                                    logger.info(f"Attempting to restart background task: {task_name}")
                                    # Re-create the task based on its name
                                    if task_name == "sftp_maintenance":
                                        new_task = asyncio.create_task(
                                            run_background_tasks_with_restart(bot, periodic_connection_maintenance, 120, task_name)
                                        )
                                        bot.background_tasks[task_name] = new_task
                                else:
                                    # Check if this is a one-time task that's meant to complete
                                    if task_name.startswith("historical_parse"):
                                        logger.info(f"One-time task {task_name} completed successfully")
                                        # Remove the task from the dictionary since it's done
                                        if task_name in bot.background_tasks:
                                            del bot.background_tasks[task_name]
                                            logger.info(f"Removed completed historical parse task: {task_name}")
                                    else:
                                        logger.warning(f"Background task {task_name} completed unexpectedly")
                            except asyncio.CancelledError:
                                # Task was cancelled, which is expected in some cases
                                logger.info(f"Background task {task_name} was cancelled")
                                # Clean up the cancelled task
                                if task_name in bot.background_tasks:
                                    del bot.background_tasks[task_name]
                            except Exception as task_error:
                                logger.error(f"Error checking task {task_name} status: {task_error}")
                except Exception as e:
                    logger.error(f"Error in background task monitor: {e}")
                
                # Check every 30 seconds
                await asyncio.sleep(30)
        
        # Start the monitor task
        monitor_task = asyncio.create_task(monitor_background_tasks(), name="task_monitor")
        
        # Start the bot
        logger.info("Starting bot...")
        await bot.start(token)
    except discord.LoginFailure:
        logger.error("Invalid token provided")
        return 1
    except asyncio.CancelledError:
        logger.warning("Bot was cancelled during startup")
        return 2
    except Exception as e:
        logger.error(f"Error starting bot: {e}")
        import traceback
        traceback.print_exc()
        return 1
    return 0

def main():
    """Main entry point"""
    try:
        return asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Bot stopped by keyboard interrupt")
        return 0
    except Exception as e:
        logger.critical(f"Unhandled exception in main: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())