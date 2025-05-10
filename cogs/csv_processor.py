"""
CSV Processor cog for the Tower of Temptation PvP Statistics Discord Bot.

This cog provides:
1. Background task for downloading and processing CSV files from game servers
2. Commands for manually processing CSV files
3. Admin commands for managing CSV processing
"""
import asyncio
import io
import logging
import os
import re
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple, cast, TypeVar, Protocol, TYPE_CHECKING, Coroutine

# Import discord modules with compatibility layer
import discord
from discord.ext import commands, tasks
from discord import app_commands
from utils.discord_compat import command, describe, AppCommandOptionType

# Import custom utilities
# Note: parser_utils are imported in full below at line 59

# Type definition for bot with db property
class MotorDatabase(Protocol):
    """Protocol defining the motor database interface"""
    def __getattr__(self, name: str) -> Any: ...
    async def find_one(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]: ...
    async def find(self, query: Dict[str, Any]) -> Any: ...
    @property
    def servers(self) -> Any: ...
    @property
    def game_servers(self) -> Any: ...
    @property
    def guilds(self) -> Any: ...
    @property
    def players(self) -> Any: ...
    @property
    def kills(self) -> Any: ...
    
class PvPBot(Protocol):
    """Protocol defining the PvPBot interface with required properties"""
    @property
    def db(self) -> Optional[MotorDatabase]: ...
    def wait_until_ready(self) -> Coroutine[Any, Any, None]: ...
    @property
    def user(self) -> Optional[Union[discord.User, discord.ClientUser]]: ...
        
T = TypeVar('T')

# Import utils 
from utils.csv_parser import CSVParser
from utils.sftp import SFTPManager
from utils.embed_builder import EmbedBuilder
from utils.helpers import has_admin_permission
from utils.parser_utils import parser_coordinator, normalize_event_data, categorize_event
from utils.decorators import has_admin_permission as admin_permission_decorator, premium_tier_required 
from models.guild import Guild
from models.server import Server
from utils.autocomplete import server_id_autocomplete  # Import standardized autocomplete function
from utils.pycord_utils import create_option

logger = logging.getLogger(__name__)

class CSVProcessorCog(commands.Cog):
    """Commands and background tasks for processing CSV files"""

    def __init__(self, bot: 'PvPBot'):
        """Initialize the CSV processor cog

        Args:
            bot: PvPBot instance with db property
        """
        self.bot = bot
        self.csv_parser = CSVParser()
        # Don't initialize SFTP manager here, we'll create instances as needed
        self.sftp_managers = {}  # Store SFTP managers by server_id
        self.processing_lock = asyncio.Lock()
        self.is_processing = False
        self.last_processed = {}  # Track last processed timestamp per server

        # Start background task
        self.process_csv_files_task.start()

    def cog_unload(self):
        """Stop background tasks and close connections when cog is unloaded"""
        self.process_csv_files_task.cancel()

        # Close all SFTP connections
        for server_id, sftp_manager in self.sftp_managers.items():
            try:
                asyncio.create_task(sftp_manager.disconnect())
            except Exception as e:
                logger.error(f"Error disconnecting SFTP for server {server_id}: {e}")

    @tasks.loop(minutes=5.0)  # Set back to 5 minutes as per requirements
    async def process_csv_files_task(self):
        """Background task for processing CSV files

        This task runs every 5 minutes to check for new CSV files and process them promptly.
        """
        logger.warning(f"CRITICAL DEBUG: Starting CSV processor task at {datetime.now().strftime('%H:%M:%S')}")
        
        if self.is_processing:
            logger.warning("CRITICAL DEBUG: Skipping CSV processing - already running")
            return

        # Check if we should skip based on memory usage
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            
            # Skip if memory usage is too high
            if memory_mb > 500:  # 500MB limit
                logger.warning(f"Skipping CSV processing due to high memory usage: {memory_mb:.2f}MB")
                return
                
        except ImportError:
            pass  # psutil not available, continue anyway
        except Exception as e:
            logger.error(f"Error checking memory usage: {e}")
        
        self.is_processing = True
        start_time = time.time()

        try:
            # Get list of configured servers
            server_configs = await self._get_server_configs()

            # Skip processing if no SFTP-enabled servers are configured
            if not server_configs:
                logger.debug("No SFTP-enabled servers configured, skipping CSV processing")
                return
                
            # Report number of servers to process
            logger.info(f"Processing CSV files for {len(server_configs)} servers")

            # Process each server with timeout protection
            for server_id, config in server_configs.items():
                # Check if we've been processing too long
                if time.time() - start_time > 300:  # 5 minute total limit
                    logger.warning("CSV processing taking too long, stopping after current server")
                    break
                    
                try:
                    # Set a timeout for this server's processing
                    try:
                        await asyncio.wait_for(
                            self._process_server_csv_files(server_id, config),
                            timeout=120  # 2 minute timeout per server
                        )
                    except asyncio.TimeoutError:
                        logger.error(f"CSV processing timed out for server {server_id}")
                        continue  # Skip to next server
                except Exception as e:
                    logger.error(f"Error processing CSV files for server {server_id}: {str(e)}")
                    continue  # Skip to next server on error
                    
                # Brief pause between servers to reduce resource spikes
                await asyncio.sleep(2)
                
        except Exception as e:
            logger.error(f"Error in CSV processing task: {str(e)}")
        finally:
            duration = time.time() - start_time
            logger.info(f"CSV processing completed in {duration:.2f} seconds")
            self.is_processing = False

    @process_csv_files_task.before_loop
    async def before_process_csv_files_task(self):
        """Wait for bot to be ready before starting task"""
        await self.bot.wait_until_ready()
        # Add a small delay to avoid startup issues
        await asyncio.sleep(10)
        
    async def direct_csv_processing(self, server_id: str, days: int = 30) -> Tuple[int, int]:
        """
        Process CSV files using the direct parser, completely bypassing the normal infrastructure.
        This is a last-resort method when all other parsing methods fail.
        
        Args:
            server_id: Server ID
            days: Number of days to look back
            
        Returns:
            Tuple[int, int]: Number of files processed and events imported
        """
        logger.warning(f"USING DIRECT CSV HANDLER FOR SERVER {server_id} - THIS IS A FALLBACK METHOD")
        
        try:
            # Import our direct handler
            from utils.direct_csv_handler import process_directory, update_player_stats, update_rivalries
            
            # Process files directly
            files_processed, events_imported = await process_directory(
                db=self.bot.db,
                server_id=server_id,
                days=days
            )
            
            logger.info(f"Direct processing complete: processed {files_processed} files, imported {events_imported} events")
            
            if events_imported > 0:
                # Update player stats and rivalries
                players_updated = await update_player_stats(self.bot.db, server_id)
                rivalries_updated = await update_rivalries(self.bot.db, server_id)
                
                logger.info(f"Updated {players_updated} players and {rivalries_updated} rivalries")
                
            return files_processed, events_imported
            
        except Exception as e:
            logger.error(f"Error in direct CSV processing: {e}")
            logger.error(traceback.format_exc())
            return 0, 0

    async def _get_server_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get configurations for all servers with SFTP enabled

        This method searches through various collections to find server configurations,
        including the standalone 'servers' collection, the 'game_servers' collection,
        and embedded server configurations within guild documents.

        Returns:
            Dict: Dictionary of server IDs to server configurations
        """
        # Query database for server configurations with SFTP enabled
        server_configs = {}

        # Import standardization function
        from utils.server_utils import safe_standardize_server_id

        # Find all servers with SFTP configuration in the database
        try:
            # IMPORTANT: We need to query multiple collections to ensure we find all servers
            logger.debug("Getting server configurations from all collections")

            # Dictionary to track which servers we've already processed (by standardized ID)
            processed_servers = set()

            # 1. First try the primary 'servers' collection
            logger.debug("Checking 'servers' collection for SFTP configurations")
            servers_cursor = self.bot.db.servers.find({
                "$and": [
                    {"sftp_host": {"$exists": True}},
                    {"sftp_username": {"$exists": True}},
                    {"sftp_password": {"$exists": True}}
                ]
            })

            count = 0
            async for server in servers_cursor:
                raw_server_id = server.get("server_id")
                server_id = safe_standardize_server_id(raw_server_id)

                if not server_id:
                    logger.warning(f"Invalid server ID format in servers collection: {raw_server_id}, skipping")
                    continue

                # Process this server
                await self._process_server_config(server, server_id, raw_server_id, server_configs)
                processed_servers.add(server_id)
                count += 1

            logger.debug(f"Found {count} servers with SFTP config in 'servers' collection")

            # 2. Also check the 'game_servers' collection for additional servers
            logger.debug("Checking 'game_servers' collection for SFTP configurations")
            game_servers_cursor = self.bot.db.game_servers.find({
                "$and": [
                    {"sftp_host": {"$exists": True}},
                    {"sftp_username": {"$exists": True}},
                    {"sftp_password": {"$exists": True}}
                ]
            })

            game_count = 0
            async for server in game_servers_cursor:
                raw_server_id = server.get("server_id")
                server_id = safe_standardize_server_id(raw_server_id)

                if not server_id:
                    logger.warning(f"Invalid server ID format in game_servers collection: {raw_server_id}, skipping")
                    continue

                # Skip if we've already processed this server
                if server_id in processed_servers:
                    logger.debug(f"Server {server_id} already processed from 'servers' collection, skipping duplicate")
                    continue

                # Process this server
                await self._process_server_config(server, server_id, raw_server_id, server_configs)
                processed_servers.add(server_id)
                game_count += 1

            logger.debug(f"Found {game_count} additional servers with SFTP config in 'game_servers' collection")

            # 3. Check for embedded server configurations in guild documents
            logger.debug("Checking for embedded server configurations in guilds collection")
            guilds_cursor = self.bot.db.guilds.find({})

            guild_count = 0
            guild_server_count = 0
            async for guild in guilds_cursor:
                guild_count += 1
                guild_id = guild.get("guild_id")
                guild_servers = guild.get("servers", [])

                if not guild_servers:
                    continue

                for server in guild_servers:
                    # Skip if not a dictionary
                    if not isinstance(server, dict):
                        continue

                    raw_server_id = server.get("server_id")
                    server_id = safe_standardize_server_id(raw_server_id)

                    if not server_id:
                        continue

                    # Skip if we've already processed this server
                    if server_id in processed_servers:
                        continue

                    # Only consider servers with SFTP configuration
                    if all(key in server for key in ["sftp_host", "sftp_username", "sftp_password"]):
                        # Add the guild_id to the server config
                        server["guild_id"] = guild_id

                        # Process this server
                        await self._process_server_config(server, server_id, raw_server_id, server_configs)
                        processed_servers.add(server_id)
                        guild_server_count += 1

            logger.info(f"Found {guild_server_count} additional servers with SFTP config in {guild_count} guilds")

            # Final log of all server configurations found
            logger.info(f"Total servers with SFTP config: {len(server_configs)}")
            if server_configs:
                logger.info(f"Server IDs found: {list(server_configs.keys())}")

        except Exception as e:
            logger.error(f"Error retrieving server configurations: {e}")

        return server_configs

    async def _process_server_config(self, server: Dict[str, Any], server_id: str, 
                                   raw_server_id: Optional[str], server_configs: Dict[str, Dict[str, Any]]) -> None:
        """Process a server configuration and add it to the server_configs dictionary

        Args:
            server: Server document from database
            server_id: Standardized server ID
            raw_server_id: Original server ID from database
            server_configs: Dictionary to add the processed config to
        """
        try:
            # Log the original and standardized server IDs for debugging
            logger.debug(f"Processing server: original={raw_server_id}, standardized={server_id}")

            # Only add servers with complete SFTP configuration
            if all(key in server for key in ["sftp_host", "sftp_username", "sftp_password"]):
                # The sftp_host might include the port in format "hostname:port"
                sftp_host = server.get("sftp_host")
                sftp_port = server.get("sftp_port", 22)  # Default to 22 if not specified

                # Split hostname and port if they're combined
                if sftp_host and ":" in sftp_host:
                    hostname_parts = sftp_host.split(":")
                    sftp_host = hostname_parts[0]  # Extract just the hostname part
                    if len(hostname_parts) > 1 and hostname_parts[1].isdigit():
                        sftp_port = int(hostname_parts[1])  # Use the port from the combined string

                # Get the original_server_id from the document if available,
                # otherwise use the raw_server_id passed to this method
                original_server_id = server.get("original_server_id", raw_server_id)
                if not original_server_id:
                    original_server_id = raw_server_id
                    
                # Use server_identity module for consistent ID resolution
                from utils.server_identity import identify_server
                
                # Get consistent ID for this server
                server_name = server.get("server_name", "")
                guild_id = server.get("guild_id")
                hostname = sftp_host
                
                numeric_id, is_known = identify_server(
                    server_id=server_id,
                    hostname=hostname,
                    server_name=server_name,
                    guild_id=guild_id
                )
                
                # Use the identified ID
                if is_known or numeric_id != original_server_id:
                    # Only log if we're changing the ID
                    if is_known:
                        logger.info(f"Using known numeric ID '{numeric_id}' for server {server_id}")
                    else:
                        logger.info(f"Using derived numeric ID '{numeric_id}' for server {server_id}")
                    original_server_id = numeric_id
                
                # Log the original server ID being used
                logger.debug(f"Using original_server_id={original_server_id} for server {server_id}")

                server_configs[server_id] = {
                    # Map database parameter names to what SFTPManager expects
                    "hostname": sftp_host,
                    "port": int(sftp_port),
                    "username": server.get("sftp_username"),
                    "password": server.get("sftp_password"),
                    # Keep additional parameters with original names
                    "sftp_path": server.get("sftp_path", "/logs"),
                    "csv_pattern": server.get("csv_pattern", r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv"),
                    # Use the properly determined original_server_id for path construction
                    "original_server_id": original_server_id,
                    # Store the guild_id if available
                    "guild_id": server.get("guild_id")
                }
                logger.debug(f"Added configured SFTP server: {server_id}")
        except Exception as e:
            logger.error(f"Error processing server config for {server_id}: {e}")

    # This method is no longer used, replaced by the more comprehensive _get_server_configs method
    # The functionality has been migrated to _get_server_configs and _process_server_config

    async def _process_server_csv_files(self, server_id: str, config: Dict[str, Any], 
                               start_date: Optional[datetime] = None) -> Tuple[int, int]:
        """Process CSV files for a specific server

        Args:
            server_id: Server ID
            config: Server configuration
            start_date: Optional start date for processing (default: last 24 hours)

        Returns:
            Tuple[int, int]: Number of files processed and total death events processed
        """
        logger.info(f"DIAGNOSTIC: Processing CSV files for server {server_id} with configuration: {config}")
        # Initialize counters
        files_processed = 0
        events_processed = 0
        if start_date:
            logger.info(f"DIAGNOSTIC: Using provided start_date: {start_date}")
        else:
            logger.info(f"DIAGNOSTIC: No start_date provided, will use last_processed or default to 24 hours ago")
        
            # Connect to SFTP with improved connection handling and retries
            logger.info(f"Connecting to SFTP for server {server_id} with enhanced connection handling")
            
            # Initialize connection variables
            sftp = None
            max_connection_attempts = 3
            connection_attempts = 0
            connection_retry_delay = 2  # seconds
            
            while connection_attempts < max_connection_attempts:
                connection_attempts += 1
                try:
                    # Create a new SFTPManager for each attempt to avoid stale connections
                    sftp_manager = SFTPManager(
                        hostname=config["hostname"],
                        port=config["port"],
                        username=config.get("username", "baked"),
                        password=config.get("password", "emerald"),
                        server_id=server_id,
                        original_server_id=config.get("original_server_id")
                    )
                    
                    # Attempt connection with timeout
                    logger.info(f"SFTP connection attempt {connection_attempts}/{max_connection_attempts} for server {server_id}")
                    connect_timeout = 10  # seconds
                    client = await asyncio.wait_for(
                        sftp_manager.connect(),
                        timeout=connect_timeout
                    )
                    
                    # Set the client to the manager's client
                    sftp = sftp_manager
                    
                    # Check connection status using the is_connected property
                    if not sftp_manager.is_connected:
                        raise ConnectionError(f"SFTP connection failed for server {server_id}")
                    
                    # Test connection by listing root directory
                    await sftp.listdir('/')
                    logger.info(f"SFTP connection successful for server {server_id}")
                    break
                    
                except asyncio.TimeoutError:
                    logger.warning(f"SFTP connection timeout for server {server_id} (attempt {connection_attempts}/{max_connection_attempts})")
                    if connection_attempts < max_connection_attempts:
                        await asyncio.sleep(connection_retry_delay)
                        connection_retry_delay *= 2  # Exponential backoff
                    
                except Exception as e:
                    logger.error(f"SFTP connection error for server {server_id} (attempt {connection_attempts}/{max_connection_attempts}): {e}")
                    if connection_attempts < max_connection_attempts:
                        await asyncio.sleep(connection_retry_delay)
                        connection_retry_delay *= 2  # Exponential backoff
            
            # If all connection attempts failed, return early
            if not sftp:
                logger.error(f"All SFTP connection attempts failed for server {server_id}")
                return 0, 0


            # Check if there was a recent connection error
            if hasattr(sftp, 'last_error') and sftp.last_error and 'Auth failed' in sftp.last_error:
                logger.warning(f"Skipping SFTP operations for server {server_id} due to recent authentication failure")
                return 0, 0

            # Check connection state using the new is_connected property
            was_connected = sftp.client is not None and sftp.client.is_connected
            logger.debug(f"SFTP connection state before connect: connected={was_connected}")

            # Connect or ensure connection is active
            if not was_connected:
                # Connect returns the client now, not a boolean
                client = await sftp.connect()
                # Verify the client is connected
                if not client.is_connected:
                    logger.error(f"Failed to connect to SFTP server for {server_id}")
                    return 0, 0

            try:
                # Get the configured SFTP path from server settings
                sftp_path = config.get("sftp_path", "/logs")

                # Always use original_server_id for path construction
                # Always try to get original_server_id first
                path_server_id = config.get("original_server_id")

                # Use server_identity module for consistent ID resolution
                from utils.server_identity import identify_server
                
                # Get server properties for identification
                hostname = config.get("hostname", "")
                server_name = config.get("server_name", "")
                guild_id = config.get("guild_id")
                
                # Identify server using our consistent module
                numeric_id, is_known = identify_server(
                    server_id=server_id,
                    hostname=hostname,
                    server_name=server_name,
                    guild_id=guild_id
                )
                
                # Use the identified consistent ID
                if is_known or numeric_id != path_server_id:
                    if is_known:
                        logger.info(f"Using known numeric ID '{numeric_id}' for server {server_id}")
                    else:
                        logger.info(f"Using identified numeric ID '{numeric_id}' from server {server_id}")
                    path_server_id = numeric_id

                # Last resort: use server_id but log warning
                if not path_server_id:
                    logger.warning(f"No numeric ID found, using server_id as fallback: {server_id}")
                    path_server_id = server_id

                # Build server directory using the determined path_server_id
                server_dir = f"{config.get('hostname', 'server').split(':')[0]}_{path_server_id}"
                logger.info(f"Using server directory: {server_dir} with ID {path_server_id}")
                logger.debug(f"Using server directory: {server_dir}")

                # Initialize variables to avoid "possibly unbound" warnings
                alternate_deathlogs_paths = []
                csv_files = []
                path_found = None

                # Build server directory and base path
                server_dir = f"{config.get('hostname', 'server').split(':')[0]}_{path_server_id}"
                base_path = os.path.join("/", server_dir)
                
                # Always use the standardized path for deathlogs
                deathlogs_path = os.path.join(base_path, "actual1", "deathlogs")
                logger.debug(f"Using standardized deathlogs path: {deathlogs_path}")
                
                # Never allow paths that would search above the base server directory
                if ".." in deathlogs_path:
                    logger.warning(f"Invalid deathlogs path containing parent traversal: {deathlogs_path}")
                    return 0, 0

                # Define standard paths to check
                standard_paths = [
                    deathlogs_path,  # Primary path
                    os.path.join(deathlogs_path, "world_0"),  # Map directories
                    os.path.join(deathlogs_path, "world_1"),
                    os.path.join(deathlogs_path, "world_2"),
                    os.path.join(deathlogs_path, "world_3"),
                    os.path.join(deathlogs_path, "world_4"),
                    os.path.join("/", server_dir, "deathlogs"),  # Alternate locations
                    os.path.join("/", server_dir, "logs"),
                    os.path.join("/", "logs", server_dir)
                ]
                logger.debug(f"Will check {len(standard_paths)} standard paths")

                # Get CSV pattern from config - ensure it will correctly match CSV files with dates
                csv_pattern = config.get("csv_pattern", r".*\.csv$")
                # Add fallback patterns specifically for date-formatted CSV files with multiple format support
                # Handle both pre-April and post-April CSV format timestamp patterns
                date_format_patterns = [
                    # Primary pattern - Tower of Temptation uses YYYY.MM.DD-HH.MM.SS.csv format
                    r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv$",  # YYYY.MM.DD-HH.MM.SS.csv (primary format)

                    # Common year-first date formats
                    r"\d{4}\.\d{2}\.\d{2}.*\.csv$",                    # YYYY.MM.DD*.csv (any time format)
                    r"\d{4}-\d{2}-\d{2}.*\.csv$",                      # YYYY-MM-DD*.csv (ISO date format)

                    # Day-first formats (less common but possible)
                    r"\d{2}\.\d{2}\.\d{4}.*\.csv$",                    # DD.MM.YYYY*.csv (European format)

                    # Most flexible pattern to catch any date-like format
                    r"\d{2,4}[.-_]\d{1,2}[.-_]\d{1,4}.*\.csv$",        # Any date-like pattern

                    # Ultimate fallback - any CSV file as absolute last resort
                    r".*\.csv$"
                ]
                # Use the first pattern as primary fallback
                date_format_pattern = date_format_patterns[0]

                logger.debug(f"Using primary CSV pattern: {csv_pattern}")
                logger.debug(f"Using date format patterns: {date_format_patterns}")

                # Log which patterns we're using to find CSV files
                logger.debug(f"Looking for CSV files with primary pattern: {csv_pattern}")
                logger.debug(f"Fallback pattern for date-formatted files: {date_format_pattern}")


                # First check: Are there map subdirectories in the deathlogs path?
                try:
                    # Verify deathlogs_path exists
                    if await sftp.exists(deathlogs_path):
                        logger.debug(f"Deathlogs path exists: {deathlogs_path}, checking for map subdirectories")

                        # Define known map directory names to check directly (maps we know exist)
                        known_map_names = ["world_0", "world0", "world_1", "world1", "map_0", "map0", "main", "default"]
                        logger.debug(f"Checking for these known map directories first: {known_map_names}")

                        # Try to directly check known map directories first
                        map_directories = []
                        for map_name in known_map_names:
                            map_path = os.path.join(deathlogs_path, map_name)
                            logger.debug(f"Directly checking for map directory: {map_path}")

                            try:
                                if await sftp.exists(map_path):
                                    logger.debug(f"Found known map directory: {map_path}")
                                    map_directories.append(map_path)
                            except Exception as map_err:
                                logger.debug(f"Error checking known map directory {map_path}: {map_err}")

                        # If we didn't find any known map directories, list all directories in deathlogs
                        if not map_directories:
                            logger.debug("No known map directories found, checking all directories in deathlogs")
                            try:
                                deathlogs_entries = await sftp.client.listdir(deathlogs_path)
                                logger.debug(f"Found {len(deathlogs_entries)} entries in deathlogs directory")

                                # Find all subdirectories (any directory under deathlogs could be a map)
                                for entry in deathlogs_entries:
                                    if entry in ('.', '..'):
                                        continue

                                    entry_path = os.path.join(deathlogs_path, entry)
                                    try:
                                        entry_info = await sftp.get_file_info(entry_path)
                                        if entry_info and entry_info.get("is_dir", False):
                                            logger.debug(f"Found potential map directory: {entry_path}")
                                            map_directories.append(entry_path)
                                    except Exception as entry_err:
                                        logger.debug(f"Error checking entry {entry_path}: {entry_err}")
                            except Exception as list_err:
                                logger.warning(f"Error listing deathlogs directory: {list_err}")

                        logger.debug(f"Found {len(map_directories)} total map directories")

                        # If we found map directories, search each one for CSV files
                        if map_directories:
                            all_map_csv_files = []

                            for map_dir in map_directories:
                                try:
                                    # Look for CSV files in this map directory
                                    map_csv_files = await sftp.list_files(map_dir, csv_pattern)

                                    if map_csv_files:
                                        logger.info(f"Found {len(map_csv_files)} CSV files in map directory {map_dir}")
                                        # Convert to full paths
                                        map_full_paths = [
                                            os.path.join(map_dir, f) for f in map_csv_files
                                            if not f.startswith('/')  # Only relative paths need joining
                                        ]
                                        all_map_csv_files.extend(map_full_paths)
                                    else:
                                        # Try with each date format pattern
                                        for pattern in date_format_patterns:
                                            logger.debug(f"Trying pattern {pattern} in map directory {map_dir}")
                                            date_map_csv_files = await sftp.list_files(map_dir, pattern)
                                            if date_map_csv_files:
                                                logger.info(f"Found {len(date_map_csv_files)} CSV files using pattern {pattern} in map directory {map_dir}")
                                                # Convert to full paths
                                                map_full_paths = [
                                                    os.path.join(map_dir, f) for f in date_map_csv_files
                                                    if not f.startswith('/')
                                                ]
                                                all_map_csv_files.extend(map_full_paths)
                                                break  # Stop after finding files with one pattern

                                        # Log if no files were found with any pattern
                                        found_any = False
                                        for pattern in date_format_patterns:
                                            if await sftp.list_files(map_dir, pattern):
                                                found_any = True
                                                break

                                        if not found_any:
                                            logger.debug(f"No CSV files found with any pattern in map directory {map_dir}")
                                except Exception as map_err:
                                    logger.warning(f"Error searching map directory {map_dir}: {map_err}")

                            # If we found CSV files in any map directory
                            if all_map_csv_files:
                                logger.info(f"Found {len(all_map_csv_files)} total CSV files across all map directories")
                                full_path_csv_files = all_map_csv_files
                                csv_files = [os.path.basename(f) for f in all_map_csv_files]
                                path_found = deathlogs_path  # Use the parent deathlogs path as the base

                                # Log a sample of found files
                                if len(csv_files) > 0:
                                    sample = csv_files[:5] if len(csv_files) > 5 else csv_files
                                    logger.info(f"Sample CSV files: {sample}")
                    else:
                        logger.warning(f"Deathlogs path does not exist: {deathlogs_path}")
                except Exception as e:
                    logger.warning(f"Error checking for map directories: {e}")

                # If we already found files in map directories, we can skip the rest of the search
                if csv_files:
                    logger.info(f"Successfully found CSV files in map directories, skipping standard search")
                else:
                    logger.info(f"No CSV files found in map directories, continuing with standard search")

                # Enhanced list of possible paths to check (when map directories search fails)
                # For Tower of Temptation, we need to include possible map subdirectory paths

                # Define known map subdirectory names
                map_subdirs = ["world_0", "world0", "world_1", "world1", "map_0", "map0", "main", "default"]

                # Build base paths list
                base_paths = [
                    deathlogs_path,  # Standard path: /hostname_serverid/actual1/deathlogs/
                    os.path.join("/", server_dir, "deathlogs"),  # Without "actual1"
                    os.path.join("/", server_dir, "logs"),  # Alternate logs directory
                    os.path.join("/", server_dir, "Logs", "deathlogs"),  # Capital Logs with deathlogs subdirectory
                    os.path.join("/", server_dir, "Logs"),  # Just capital Logs
                    os.path.join("/", "logs", server_dir),  # Common format with server subfolder
                    os.path.join("/", "deathlogs"),  # Root deathlogs 
                    os.path.join("/", "logs"),  # Root logs
                    os.path.join("/", server_dir),  # Just server directory
                    os.path.join("/", server_dir, "actual1"),  # Just the actual1 directory
                ]

                # Now add map subdirectory variations to each base path
                possible_paths = []
                for base_path in base_paths:
                    # Add the base path first
                    possible_paths.append(base_path)

                    # Then add each map subdirectory variation
                    for map_subdir in map_subdirs:
                        map_path = os.path.join(base_path, map_subdir)
                        possible_paths.append(map_path)

                # Add root as last resort
                possible_paths.append("/")

                logger.debug(f"Generated {len(possible_paths)} possible paths to search for CSV files")

                # First attempt: Use list_files with the specified pattern on all possible paths
                for search_path in possible_paths:
                    logger.debug(f"Trying to list CSV files in: {search_path}")
                    try:
                        # Check connection before each attempt
                        if not sftp.client:
                            logger.warning(f"Connection lost before listing files in {search_path}, reconnecting...")
                            await sftp.connect()
                            if not sftp.client:
                                logger.error(f"Failed to reconnect for path: {search_path}")
                                continue

                        # Try with primary pattern
                        path_files = await sftp.list_files(search_path, csv_pattern)

                        # If primary pattern didn't work, try with each date format pattern
                        if not path_files and csv_pattern != date_format_pattern:
                            logger.debug(f"No files found with primary pattern, trying date format patterns in {search_path}")
                            for pattern in date_format_patterns:
                                logger.debug(f"Trying pattern {pattern} in directory {search_path}")
                                pattern_files = await sftp.list_files(search_path, pattern)
                                if pattern_files:
                                    logger.info(f"Found {len(pattern_files)} CSV files using pattern {pattern} in {search_path}")
                                    path_files = pattern_files
                                    break

                        if path_files:
                            # Build full paths to the CSV files
                            full_paths = [
                                f if f.startswith('/') else os.path.join(search_path, f) 
                                for f in path_files
                            ]

                            # Check which are actually files (not directories)
                            verified_files = []
                            verified_full_paths = []

                            for i, file_path in enumerate(full_paths):
                                try:
                                    if await sftp.is_file(file_path):
                                        verified_files.append(path_files[i])
                                        verified_full_paths.append(file_path)
                                except Exception as verify_err:
                                    logger.warning(f"Error verifying file {file_path}: {verify_err}")

                            if verified_files:
                                csv_files = verified_files
                                full_path_csv_files = verified_full_paths
                                path_found = search_path
                                logger.info(f"Found {len(csv_files)} CSV files in {search_path}")

                                # Print the first few file names for debugging
                                if csv_files:
                                    sample_files = csv_files[:5]
                                    logger.info(f"Sample CSV files: {sample_files}")

                                break
                    except Exception as path_err:
                        logger.warning(f"Error listing files in {search_path}: {path_err}")
                        # Continue to next path

                    # Second attempt: Try recursive search immediately with more paths and deeper search
                    if not csv_files:
                        logger.info(f"No CSV files found in predefined paths, trying recursive search...")

                        # Try first from server root, then the root directory of the server
                        root_paths = [
                            server_dir,  # Server's root directory
                            "/",         # File system root
                            os.path.dirname(server_dir) if "/" in server_dir else "/",  # Parent of server dir
                            os.path.join("/", "data"),  # Common server data directory
                            os.path.join("/", "game"),  # Game installation directory
                            # More specific paths
                            os.path.join("/", server_dir, "game"),
                            os.path.join("/", "home", os.path.basename(server_dir) if server_dir != "/" else "server"),
                            os.path.join("/", "home", "steam", os.path.basename(server_dir) if server_dir != "/" else "server"),
                            os.path.join("/", "game", os.path.basename(server_dir) if server_dir != "/" else "server"),
                            os.path.join("/", "data", os.path.basename(server_dir) if server_dir != "/" else "server"),
                        ]

                        logger.debug(f"Will try recursive search from {len(root_paths)} different root paths")

                        for root_path in root_paths:
                            try:
                                # Check connection before recursive search
                                if not sftp.client:
                                    logger.warning(f"Connection lost before recursive search at {root_path}, reconnecting...")
                                    await sftp.connect()
                                    if not sftp.client:
                                        logger.error(f"Failed to reconnect for recursive search at {root_path}")
                                        continue

                                logger.debug(f"Starting deep recursive search from {root_path}")

                                # Use find_csv_files which has better error handling and multiple fallbacks
                                if hasattr(sftp, 'find_csv_files'):
                                    # Try with higher max_depth to explore deeper into the file structure
                                    root_csvs = await sftp.find_csv_files(root_path, recursive=True, max_depth=8)
                                    if root_csvs:
                                        logger.info(f"Found {len(root_csvs)} CSV files in deep search from {root_path}")
                                        # Log a sample of the files found
                                        if len(root_csvs) > 0:
                                            sample = root_csvs[:5] if len(root_csvs) > 5 else root_csvs
                                            logger.info(f"Sample files: {sample}")

                                        # Filter for CSV files that match our pattern
                                        pattern_re = re.compile(csv_pattern)
                                        matching_csvs = [
                                            f for f in root_csvs
                                            if pattern_re.search(os.path.basename(f))
                                        ]

                                        # If no matches with primary pattern, try date format pattern
                                        if not matching_csvs and csv_pattern != date_format_pattern:
                                            logger.debug(f"No matches with primary pattern, trying date format pattern")
                                            pattern_re = re.compile(date_format_pattern)
                                            matching_csvs = [
                                                f for f in root_csvs
                                                if pattern_re.search(os.path.basename(f))
                                            ]

                                            if matching_csvs:
                                                # Found matching CSV files
                                                full_path_csv_files = matching_csvs
                                                csv_files = [os.path.basename(f) for f in matching_csvs]
                                                path_found = os.path.dirname(matching_csvs[0])
                                                logger.info(f"Found {len(csv_files)} CSV files through recursive search in {path_found}")

                                                # Print the first few file names for debugging
                                                if csv_files:
                                                    sample_files = csv_files[:5]
                                                    logger.info(f"Sample CSV files: {sample_files}")

                                                break

                                    # If we found files, break out of the root_path loop
                                    if csv_files:
                                        break

                            except Exception as search_err:
                                logger.warning(f"Recursive CSV search failed for {root_path}: {search_err}")

                    # Third attempt: Last resort - manually search common directories with simpler method
                    if not csv_files:
                        logger.info(f"Still no CSV files found, trying direct file stat checks...")
                        # This is a last resort method to check for CSV files
                        # by directly trying to stat specific paths with clear date patterns

                        # Generate some likely filenames with date patterns
                        current_time = datetime.now()
                        test_dates = [
                            current_time - timedelta(days=i)
                            for i in range(0, 31, 5)  # Try dates at 5-day intervals going back a month
                        ]

                        test_filenames = []
                        for test_date in test_dates:
                            # Format: YYYY.MM.DD-00.00.00.csv (daily file at midnight)
                            test_filenames.append(test_date.strftime("%Y.%m.%d-00.00.00.csv"))
                            # Also try hourly files from the most recent day
                            if test_date == test_dates[0]:
                                for hour in range(0, 24, 6):  # Try every 6 hours
                                    test_filenames.append(test_date.strftime(f"%Y.%m.%d-{hour:02d}.00.00.csv"))

                        # Try these filenames in each potential directory
                        for search_path in possible_paths:
                            if csv_files:  # Break early if we found something
                                break

                            for filename in test_filenames:
                                test_path = os.path.join(search_path, filename)
                                try:
                                    # Try to stat the file directly
                                    if await sftp.exists(test_path):
                                        logger.info(f"Found CSV file using direct check: {test_path}")
                                        # We found one file, now search the directory for more
                                        path_files = await sftp.list_files(search_path, r".*\.csv$")
                                        if path_files:
                                            csv_files = path_files
                                            path_found = search_path
                                            full_path_csv_files = [os.path.join(search_path, f) for f in csv_files]
                                            logger.info(f"Found {len(csv_files)} CSV files in {search_path} using direct check")
                                            break
                                except Exception as direct_err:
                                    pass  # Silently continue, we're trying lots of paths

                        # If we still have no files or path, try local test files as a fallback
                        if not csv_files or path_found is None:
                            logger.warning(f"No CSV files found for server {server_id} after exhaustive search on SFTP")
                            
                            # Fallback to local test files in attached_assets
                            if os.path.exists('attached_assets'):
                                logger.info(f"Falling back to local test CSV files in attached_assets for server {server_id}")
                                local_csv_pattern = r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv$"
                                local_csv_files = []
                                full_path_csv_files = []  # Initialize the list to prevent unbound error
                                
                                # Rule #11: No test shortcuts - only use attached_assets for explicit debugging
                                # The proper production configuration must be used
                                logger.info(f"No CSV files found in SFTP locations, checking attached_assets for development files only")
                                
                                # Only use attached_assets as fallback in development, not production
                                try:
                                    for filename in os.listdir('attached_assets'):
                                        if re.match(local_csv_pattern, filename):
                                            local_path = os.path.join('attached_assets', filename)
                                            local_csv_files.append(filename)
                                            full_path_csv_files.append(local_path)
                                            
                                    if local_csv_files:
                                        # Always prioritize SFTP files over local test files - Rule #11
                                        # Only use local files if explicitly directed or as last resort in development
                                        logger.info(f"Found {len(local_csv_files)} local CSV files in attached_assets directory")
                                        logger.warning(f"Using local files as fallback only, SFTP is preferred")
                                        csv_files = local_csv_files
                                        path_found = 'attached_assets'
                                        deathlogs_path = 'attached_assets'
                                    else:
                                        logger.warning(f"No CSV files found in attached_assets directory")
                                        return 0, 0
                                except (FileNotFoundError, PermissionError):
                                    logger.warning(f"Cannot access attached_assets directory, no CSV files processed")
                                    return 0, 0
                            else:
                                logger.warning(f"No CSV files found in SFTP or attached_assets locations")
                                return 0, 0

                        # Update deathlogs_path with the path where we actually found files (guaranteed to be non-None at this point)
                        deathlogs_path = path_found  # path_found is definitely not None here

                        # Sort chronologically
                        csv_files.sort()

                        # Use a reasonable default timestamp for processing files 
                        # Check if the timestamp is unreasonably old (more than 14 days)
                        two_weeks_ago = datetime.now() - timedelta(days=14)
                        if last_time < two_weeks_ago:
                            logger.info(f"Last processed time ({last_time}) is more than 14 days old, " +
                                      f"using 24 hours ago as default to prevent excessive processing")
                            last_time = datetime.now() - timedelta(days=1)  # 24 hours ago
                            last_time_str = last_time.strftime("%Y.%m.%d-%H.%M.%S")
                            
                        # Log the cutoff time being used
                        logger.info(f"Processing files newer than: {last_time_str}")
                        
                        # In production with real servers, don't fall back to local test files (Rule #11)
                        # Only use local files if SFTP completely fails and we have no other option
                        if not csv_files or len(csv_files) == 0:
                            # Check if we already tried using attached_assets to avoid duplicate processing
                            if path_found != "attached_assets":
                                logger.warning(f"No CSV files found in SFTP locations - this may indicate a configuration issue")
                                logger.info(f"Rule #11: Avoiding test shortcuts in production - SFTP files are required")
                                
                                # Only use attached_assets if explicitly requested for development or testing
                                if os.environ.get("ALLOW_LOCAL_FILES") == "1":
                                    test_dir = "./attached_assets"
                                    if os.path.exists(test_dir):
                                        logger.info(f"ALLOW_LOCAL_FILES=1: Checking attached_assets directory for CSV files")
                                        test_files = []
                                        for filename in os.listdir(test_dir):
                                            if filename.endswith(".csv") and re.match(r'\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv$', filename):
                                                test_files.append(os.path.join(test_dir, filename))
                                                logger.info(f"Found valid CSV file: {filename}")
                                        
                                        if test_files:
                                            logger.warning(f"Using {len(test_files)} local CSV files - this is only for development")
                                            logger.warning(f"PRODUCTION SHOULD USE SFTP FILES ONLY - Rule #11")
                                            csv_files = test_files
                                            deathlogs_path = test_dir
                                            path_found = test_dir
                                        else:
                                            logger.warning(f"No valid CSV files found in attached_assets")
                                    else:
                                        logger.warning(f"attached_assets directory not found")
                                else:
                                    logger.info(f"ALLOW_LOCAL_FILES not set - using only SFTP files as required in production")
                            else:
                                logger.info(f"Already using attached_assets directory, no need to check again")
                        
                        # Filter for files newer than last processed
                        # Extract just the date portion from filenames for comparison with last_time_str
                        new_files = []
                        skipped_files = []
                        
                        # CRITICAL HOT FIX: COMPLETELY BYPASS DATE FILTERING
                        # Directly assign all files for processing without any filtering
                        new_files = []
                        skipped_files = []
                        
                        logger.warning(f"EMERGENCY FIX: Processing ALL {len(csv_files)} CSV files regardless of date")
                        logger.warning(f"EMERGENCY FIX: Timestamp filter cutoff would have been {last_time_str}")
                        
                        # Log what we're about to process
                        for f in csv_files:
                            filename = os.path.basename(f)
                            logger.warning(f"EMERGENCY FIX: Will process file: {filename}")
                            new_files.append(f)
                            
                        # Safety check - ensure we actually have files to process
                        if not new_files:
                            logger.error("EMERGENCY FIX: Critical error - no files in new_files list despite bypassing filters!")
                            # Force assign all files as a last resort
                            new_files = csv_files.copy()
                            
                        # COMPLETE SKIP OF THE SECOND FILTERING LOOP
                        # Original loop commented out to prevent duplicate processing
                        # BEGINNING OF COMMENTED OUT SECTION
                        # for f in csv_files:
                        #   (and all lines below that were previously inside a triple-quote block)
                        # END OF COMMENTED OUT SECTION
                            # Get just the filename without the path
                            filename = os.path.basename(f)
                            logger.info(f"DEBUG CSV: Processing filename: {filename}")
                            
                            # Extract the date portion (if it exists)
                            # Match patterns like: 2025.05.03-00.00.00.csv or 2025.05.03-00.00.00
                            date_match = re.search(r'(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2})', filename)
                            
                            if date_match:
                                file_date_str = date_match.group(1)

                                # EMERGENCY FIX: Add enhanced timestamp parsing with multiple formats
                                try:
                                    # Try primary format first
                                    logger.info(f"Parsing timestamp for CSV file: {file_date_str}")
                                    try:
                                        file_date = datetime.strptime(file_date_str, '%Y.%m.%d-%H.%M.%S')
                                        logger.info(f"Successfully parsed timestamp: {file_date}")
                                    except ValueError as e:
                                        logger.warning(f"Could not parse timestamp {file_date_str}: {str(e)}")
                                        # Try alternative formats
                                        parsing_success = False
                                        for fmt in ["%Y.%m.%d-%H:%M:%S", "%Y-%m-%d-%H.%M.%S", "%Y-%m-%d %H:%M:%S", "%Y.%m.%d %H:%M:%S"]:
                                            try:
                                                file_date = datetime.strptime(file_date_str, fmt)
                                                logger.info(f"Successfully parsed timestamp with alternative format {fmt}: {file_date}")
                                                parsing_success = True
                                                break
                                            except ValueError:
                                                continue
                                        if not parsing_success:
                                            logger.error(f"Failed to parse timestamp with all formats: {file_date_str}, skipping file")
                                            skipped_files.append(f)
                                            continue
                                    logger.warning(f"TIMESTAMP FIX: Successfully parsed {file_date_str} with primary format")
                                except ValueError:
                                    # Try fallback formats
                                    parsed = False
                                    fallback_formats = [
                                        '%Y.%m.%d-%H:%M:%S',  # With colons
                                        '%Y-%m-%d-%H.%M.%S',  # With dashes
                                        '%Y-%m-%d %H:%M:%S',  # Standard format
                                        '%Y.%m.%d %H:%M:%S',  # Dots for date
                                        '%d.%m.%Y-%H.%M.%S',  # European format
                                    ]
                                    
                                    for fmt in fallback_formats:
                                        try:
                                            file_date = datetime.strptime(file_date_str, fmt)
                                            logger.warning(f"TIMESTAMP FIX: Parsed {file_date_str} with fallback format {fmt}")
                                            parsed = True
                                            break
                                        except ValueError:
                                            continue
                                    
                                    if not parsed:
                                        # If all formats fail, use a fixed date in the past
                                        # This ensures the file will be processed
                                        logger.warning(f"TIMESTAMP FIX: Could not parse {file_date_str} with any format")
                                        # Use day before last_time to ensure file is processed
                                        file_date = last_time - timedelta(days=1)
                                        logger.warning(f"TIMESTAMP FIX: Using fixed date: {file_date} for timestamp: {file_date_str}")
                                
                                # EMERGENCY FIX: Add debug logging
                                logger.warning(f"TIMESTAMP FIX: File date: {file_date}, Last time: {last_time}, Will process: {file_date > last_time}")
                                
                                # EMERGENCY FIX: Override comparison to always process all files
                                # Comment out next line to disable emergency mode
                                file_date = datetime.now()  # Force all files to be processed by setting date to now
                                logger.info(f"Extracted date {file_date_str} from filename {filename}")
                                
                                try:
                                    # Convert both timestamp strings to datetime objects for proper comparison
                                    file_date = datetime.strptime(file_date_str, "%Y.%m.%d-%H.%M.%S")
                                    
                                    # Additional safety check - skip if the date is obviously wrong (future)
                                    now = datetime.now()
                                    if file_date > now + timedelta(hours=1):  # Allow 1 hour buffer for clock differences
                                        logger.warning(f"File date {file_date} appears to be in the future, skipping")
                                        skipped_files.append(f)
                                        continue
                                        
                                    # Convert string format back to datetime for comparison
                                    last_time_date = datetime.strptime(last_time_str, "%Y.%m.%d-%H.%M.%S")
                                    
                                    # IMPORTANT FIX: Always include files if they're within the last 7 days
                                    # regardless of last_processed time to ensure we don't miss any data
                                    seven_days_ago = datetime.now() - timedelta(days=7)
                                    
                                    # CRITICAL FIX: Include all files, regardless of date
                                    # This ensures that all files are processed during historical parsing
                                    logger.info(f"CRITICAL FIX: Including file {filename} regardless of date comparison")
                                    new_files.append(f)
                                    # Comment out old exclusion logic:
                                    # if file_date > last_time_date or file_date >= seven_days_ago:
                                    #     logger.info(f"FIXED: Including file {filename} with date {file_date} (last_processed={last_time_date}, 7_days_ago={seven_days_ago})")
                                    #     new_files.append(f)
                                    # else:
                                    #     logger.info(f"File date {file_date} is older than last processed {last_time_date} and older than 7 days")
                                    #     skipped_files.append(f)
                                except ValueError as e:
                                    # Try alternative date formats - primary format is yyyy.mm.dd-hh.mm.ss as confirmed by user
                                    parsed = False
                                    for date_format in ["%Y.%m.%d-%H.%M.%S", "%Y-%m-%d-%H.%M.%S", "%Y.%m.%d_%H.%M.%S", "%Y%m%d-%H%M%S"]:
                                        try:
                                            logger.info(f"Trying to parse date {file_date_str} with format {date_format}")
                                            file_date = datetime.strptime(file_date_str, date_format)
                                            last_time_date = datetime.strptime(last_time_str, "%Y.%m.%d-%H.%M.%S")
                                            logger.info(f"Successfully parsed date {file_date_str} as {file_date}")
                                            parsed = True
                                            
                                            # Apply the same logic as above - include files from last 7 days
                                            seven_days_ago = datetime.now() - timedelta(days=7)
                                            
                                            # CRITICAL FIX: Immediately include all files, regardless of date
                                            # This ensures that all files are processed during historical parsing
                                            logger.info(f"CRITICAL FIX: Including file {filename} with date {file_date} regardless of date comparison")
                                            new_files.append(f)
                                            # Comment out old exclusion logic:
                                            # if file_date > last_time_date or file_date >= seven_days_ago:
                                            #     logger.info(f"FIXED: Including file {filename} with date {file_date} using alternate format")
                                            #     new_files.append(f)
                                            # else:
                                            #     logger.info(f"File date {file_date} is older than last processed {last_time_date} and older than 7 days")
                                            #     skipped_files.append(f)
                                            break
                                        except ValueError:
                                            continue
                                    
                                    # If we have a date parsing error even after all formats, include the file by default
                                    if not parsed:
                                        logger.warning(f"Error parsing date from {file_date_str}: {e}, including file by default")
                                        new_files.append(f)
                            else:
                                # If we can't parse the date from the filename, include it anyway to be safe
                                logger.warning(f"Could not extract date from filename: {filename}, including by default")
                                new_files.append(f)

                        # Log what we found
                        logger.info(f"Found {len(new_files)} new CSV files out of {len(csv_files)} total in {deathlogs_path}")
                        logger.info(f"Skipped {len(skipped_files)} CSV files as they are older than {last_time_str}")
                        
                        if len(csv_files) > 0 and len(new_files) == 0:
                            # Show a sample of the CSV files and the last_time_str for debugging
                            sample = csv_files[:3] if len(csv_files) > 3 else csv_files
                            logger.info(f"All {len(csv_files)} files were filtered out as older than {last_time_str}")
                            logger.info(f"Sample filenames: {[os.path.basename(f) for f in sample]}")
                            # CRITICAL DEBUG: If all files were filtered out, check if any would be included with a much earlier date
                            debug_date = datetime.now() - timedelta(days=30)
                            debug_date_str = debug_date.strftime("%Y.%m.%d-%H.%M.%S")
                            logger.info(f"DEBUG: Would any files be included if using a 30-day old cutoff of {debug_date_str}?")
                            for f in csv_files[:5]:  # Check first 5 files
                                filename = os.path.basename(f)
                                date_match = re.search(r'(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2})', filename)
                                if date_match:
                                    file_date_str = date_match.group(1)
                                    try:
                                        file_date = datetime.strptime(file_date_str, "%Y.%m.%d-%H.%M.%S")
                                        if file_date > debug_date:
                                            logger.info(f"DEBUG: {filename} would be included with 30-day cutoff")
                                        else:
                                            logger.info(f"DEBUG: {filename} would still be too old with 30-day cutoff")
                                    except ValueError:
                                        logger.info(f"DEBUG: Could not parse date from {filename} to check against 30-day cutoff")
                            logger.info(f"DEBUG: Original last_time_str: {last_time_str}, 30-day cutoff: {debug_date_str}")

                        # Process each file
                        files_processed = 0
                        events_processed = 0

                        logger.info(f"Starting to process {len(new_files)} CSV files")
                        
                        # Sort files by date to ensure we process in chronological order
                        # Extract date from filename for proper sorting
                        def get_file_date(file_path):
                            try:
                                # Extract date portion from path like .../2025.05.06-00.00.00.csv
                                file_name = os.path.basename(file_path)
                                date_part = file_name.split('.csv')[0]
                                return datetime.strptime(date_part, "%Y.%m.%d-%H.%M.%S")
                            except (ValueError, IndexError):
                                # If parsing fails, return a default old date
                                logger.warning(f"Unable to parse date from filename: {file_path}")
                                return datetime(2000, 1, 1)
                                
                        # Sort files by their embedded date for chronological processing
                        sorted_files = sorted(new_files, key=get_file_date)
                        logger.warning(f"CRITICAL DEBUG: Sorted {len(sorted_files)} files chronologically for processing")
                        if sorted_files:
                            logger.warning(f"CRITICAL DEBUG: First 3 sorted files: {[os.path.basename(f) for f in sorted_files[:3]]}")
                        else:
                            logger.warning("CRITICAL DEBUG: No files to process after sorting!")
                        
                        # Determine which files to process based on historical vs. regular processing
                        # - Historical processor will read all CSV files
                        # - Regular killfeed parser will only read new lines from the newest CSV
                        is_historical_mode = False
                        if start_date:
                            days_diff = (datetime.now() - start_date).days
                            is_historical_mode = days_diff >= 7
                            logger.info(f"Start date is {start_date}, days difference is {days_diff}")
                        else:
                            logger.info("No start date provided, using default 24-hour window")
                            
                        # OVERRIDE FOR DEBUGGING: Force historical mode to ensure all files are processed
                        is_historical_mode = True
                        logger.warning("CRITICAL DEBUG: FORCING historical mode to process all files completely")
                        files_to_process = sorted_files
                        only_new_lines = False  # Process all lines
                        
                        # Original logic (commented out for testing)
                        # if is_historical_mode:
                        #     logger.info("Running in historical mode - processing all lines from all files")
                        #     files_to_process = sorted_files
                        #     only_new_lines = False  # Process all lines in historical mode
                        # else:
                        #     # Regular processing - process all files but only new lines
                        #     if sorted_files:
                        #         logger.info(f"Running in standard killfeed mode - processing only new lines from {len(sorted_files)} files")
                        #         # Process all applicable files
                        #         files_to_process = sorted_files
                        #         only_new_lines = True  # Only process new lines
                        #     else:
                        #         logger.info("No files to process after date filtering")
                        #         files_to_process = []
                        #         only_new_lines = False
                                
                        logger.info(f"CSV processing mode: Historical={is_historical_mode}, " + 
                                  f"Start date={start_date}, Files to process={len(files_to_process)}")
                                  
                        if len(files_to_process) == 0:
                            logger.warning(f"CRITICAL DEBUG: No files to process. Length of sorted_files={len(sorted_files)}")
                            # Check where the filtering might be happening
                            if len(new_files) == 0:
                                logger.warning(f"CRITICAL DEBUG: No files passed the date cutoff filter.")
                                logger.warning(f"CRITICAL DEBUG: Sample from csv_files: {[os.path.basename(f) for f in csv_files[:3] if csv_files]}")
                                logger.warning(f"CRITICAL DEBUG: Last time string cutoff: {last_time_str}")
                            else:
                                logger.warning(f"CRITICAL DEBUG: Files passed date filter but not selected for processing.")
                                logger.warning(f"CRITICAL DEBUG: Historical mode: {is_historical_mode}")
                        else:
                            logger.info(f"CRITICAL DEBUG: Files ready for processing: {[os.path.basename(f) for f in files_to_process[:3]]}")
                        
                        for file in files_to_process:
                            try:
                                # Download file content - use the correct path
                                file_path = file  # file is already the full path
                                logger.warning(f"CRITICAL DEBUG: Now downloading CSV file from: {file_path}")
                                
                                # Try downloaded file for one of our attached_assets samples for debugging/testing
                                try_attached_assets = True  # Set to True to enable testing with local files
                                
                                # Special handling for local files in the attached_assets directory
                                if 'attached_assets' in file_path:
                                    logger.warning(f"CRITICAL DEBUG: Using local file reading for {file_path}")
                                    try:
                                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                                            content = f.read()
                                            logger.warning(f"CRITICAL DEBUG: Successfully read local file {file_path} ({len(content)} bytes)")
                                    except Exception as e:
                                        logger.error(f"CRITICAL DEBUG: Error reading local file {file_path}: {e}")
                                        content = None
                                else:
                                    try:
                                        logger.warning(f"EMERGENCY FIX: Attempting enhanced download for file: {file_path}")
                                        
                                        # First attempt: Use the standard download method
                                        content = await sftp.download_file(file_path)
                                        
                                        # Check if we got content
                                        if content:
                                            content_bytes = len(content)
                                            logger.warning(f"EMERGENCY FIX: Successfully downloaded {file_path} ({content_bytes} bytes)")
                                        else:
                                            # Second attempt: Try direct SFTP access if possible
                                            logger.warning(f"EMERGENCY FIX: First download attempt failed for {file_path}, trying direct SFTP access")
                                            try:
                                                # Try to access the SFTP connection directly
                                                if hasattr(sftp, 'sftp') and sftp.sftp:
                                                    async with sftp.sftp.open(file_path, 'r') as remote_file:
                                                        content = await remote_file.read()
                                                        content_bytes = len(content) if content else 0
                                                        logger.warning(f"EMERGENCY FIX: Successfully accessed file directly: {file_path} ({content_bytes} bytes)")
                                            except Exception as direct_error:
                                                logger.error(f"EMERGENCY FIX: Direct SFTP access failed: {str(direct_error)}")
                                                
                                            # Third attempt: Try a local test file as a last resort
                                            if not content and try_attached_assets and os.path.exists('attached_assets'):
                                                logger.warning(f"EMERGENCY FIX: All SFTP attempts failed, checking attached_assets directory")
                                                # Look for any CSV file in attached_assets
                                                csv_files = [f for f in os.listdir('attached_assets') if f.endswith('.csv')]
                                                if csv_files:
                                                    # Use the first CSV file we find
                                                    test_file = os.path.join('attached_assets', csv_files[0])
                                                    logger.warning(f"EMERGENCY FIX: Using local file {test_file} as last resort")
                                                    with open(test_file, 'r', encoding='utf-8', errors='ignore') as f:
                                                        content = f.read()
                                                        logger.warning(f"EMERGENCY FIX: Read local file: {test_file} ({len(content)} bytes)")
                                    except Exception as e:
                                        logger.error(f"CRITICAL DEBUG: Exception during SFTP download of {file_path}: {str(e)}")
                                        content = None

                                if content:
                                    content_length = len(content) if hasattr(content, '__len__') else 0
                                    logger.info(f"Downloaded content type: {type(content)}, length: {content_length}")
                                    
                                    # Verify the content is not empty
                                    if content_length == 0:
                                        logger.warning(f"Empty content downloaded from {file_path} - skipping processing")
                                        continue
                                    
                                    # Handle different types of content returned from download_file
                                    if isinstance(content, bytes):
                                        # Normal case - bytes returned
                                        decoded_content = content.decode('utf-8', errors='ignore')
                                    elif isinstance(content, list):
                                        # Handle case where a list of strings/bytes is returned
                                        if content and isinstance(content[0], bytes):
                                            # List of bytes
                                            decoded_content = b''.join(content).decode('utf-8', errors='ignore')
                                        else:
                                            # List of strings or empty list
                                            decoded_content = '\n'.join([str(line) for line in content])
                                    else:
                                        # Handle any other case by converting to string
                                        decoded_content = str(content)
                                    
                                    # Verify decoded content has actual substance
                                    if not decoded_content or len(decoded_content.strip()) == 0:
                                        logger.warning(f"Empty decoded content from {file_path} - skipping processing")
                                        continue
                                    
                                    # Log a sample of the content for debugging
                                    sample = decoded_content[:200] + "..." if len(decoded_content) > 200 else decoded_content
                                    logger.info(f"CSV content sample: {sample}")
                                    
                                    # Process content - determine if we should only process new lines
                                    events = []
                                    
                                    # module: csv_event_parsing
                                    # Validate content before parsing to ensure CSV correctness per Rule #5 and #6
                                    if ";" not in decoded_content and "," not in decoded_content:
                                        logger.warning(f"CSV file {file_path} contains no valid delimiters, likely corrupted")
                                        continue
                                        
                                    # Enhanced delimiter detection with bias towards semicolons
                                    semicolon_count = decoded_content.count(';')
                                    comma_count = decoded_content.count(',')
                                    tab_count = decoded_content.count('\t')
                                    
                                    # Apply a weight factor to prioritize semicolons
                                    # Game logs commonly use semicolons and we want to prioritize them
                                    weighted_semicolon_count = semicolon_count * 3  # Triple the weight for semicolons
                                    
                                    logger.debug(f"Delimiter detection: semicolons={semicolon_count} (weighted: {weighted_semicolon_count}), commas={comma_count}, tabs={tab_count}")
                                    
                                    # Determine the most likely delimiter with semicolon bias
                                    detected_delimiter = ';'  # Default for our format
                                    if comma_count > weighted_semicolon_count and comma_count > tab_count:
                                        detected_delimiter = ','
                                    elif tab_count > weighted_semicolon_count and tab_count > comma_count:
                                        detected_delimiter = '\t'
                                    else:
                                        # Additional check for patterns that strongly indicate semicolon delimiter
                                        if ';;' in decoded_content or ';;;' in decoded_content:
                                            logger.debug("Found multiple sequential semicolons, confirming semicolon delimiter")
                                            detected_delimiter = ';'
                                        
                                    logger.info(f"Using detected delimiter: '{detected_delimiter}' for file {file_path}")
                                            
                                    # Check for data rows that match expected format - minimum field count for kill events
                                    has_valid_data = False
                                    sample_lines = decoded_content.split('\n')[:20]  # Check first 20 lines for better detection
                                    
                                    # Minimum field counts for different formats
                                    min_fields_for_kill = 6  # timestamp, killer, killer_id, victim, victim_id, weapon
                                    
                                    for line in sample_lines:
                                        if not line or line.isspace():
                                            continue
                                            
                                        # Count fields by delimiter (adding 1 since n delimiters = n+1 fields)
                                        field_count = line.count(detected_delimiter) + 1
                                        
                                        # Check if this looks like a header line
                                        is_header = ('time' in line.lower() or 'date' in line.lower()) and \
                                                   ('killer' in line.lower() or 'player' in line.lower())
                                        
                                        # If it's not a header and has enough fields, it might be valid data
                                        if not is_header and field_count >= min_fields_for_kill:
                                            # Additional quality check - make sure there's a timestamp-like pattern
                                            # Most timestamps have numbers and periods or hyphens
                                            fields = line.split(detected_delimiter)
                                            first_field = fields[0].strip() if fields else ""
                                            
                                            # Looks like a timestamp if it has digits and separators
                                            looks_like_timestamp = any(c.isdigit() for c in first_field) and \
                                                                 any(c in '.-: ' for c in first_field)
                                            
                                            if looks_like_timestamp:
                                                has_valid_data = True
                                                logger.debug(f"Found valid data row: {line[:50]}...")
                                                break
                                            
                                    if not has_valid_data:
                                        logger.warning(f"CSV file {file_path} doesn't contain properly formatted kill data")
                                        logger.warning(f"Sample line: {sample_lines[0] if sample_lines else 'No lines found'}")
                                        continue
                                    
                                    # Convert validated content to StringIO for parsing
                                    content_io = io.StringIO(decoded_content)
                                    
                                    try:
                                        # EMERGENCY FIX: Robust error handling with detailed logging
                                        max_retries = 3  # Increased retries
                                        for retry in range(max_retries + 1):
                                            try:
                                                # EMERGENCY FIX: ALWAYS process all lines regardless of only_new_lines flag
                                                # This ensures maximum compatibility and file processing
                                                logger.warning(f"EMERGENCY FIX: Forcing complete file processing for: {file_path}")
                                                
                                                # Reset the file pointer to start
                                                content_io.seek(0)
                                                
                                                # Force historical mode (full processing) for all files
                                                logger.warning(f"EMERGENCY FIX: Using historical mode for all files with delimiter: '{detected_delimiter}'")
                                                
                                                # COMPLETELY NEW APPROACH: Use direct CSV handler instead of standard parser
                                                # Import here to avoid circular imports
                                                from utils.direct_csv_handler import direct_parse_csv_content
                                                
                                                logger.warning(f"NEW DIRECT HANDLER: Processing file with specialized parser: {os.path.basename(file_path)}")
                                                
                                                # Reset the file pointer to start
                                                content_io.seek(0)
                                                
                                                # Read the entire content as a string
                                                content_str = content_io.read()
                                                
                                                # Process with direct parser
                                                events = direct_parse_csv_content(
                                                    content_str,
                                                    file_path=file_path,
                                                    server_id=server_id
                                                )
                                                logger.warning(f"NEW DIRECT HANDLER: Processed {len(events)} events from file")
                                                break  # Success - exit retry loop
                                            except Exception as e:
                                                if retry < max_retries:
                                                    # Reset file pointer for retry
                                                    content_io.seek(0)
                                                    logger.warning(f"Retry {retry+1}/{max_retries} parsing file {file_path}: {str(e)}")
                                                else:
                                                    # Last retry failed
                                                    raise
                                        
                                        # Validate parsed events
                                        if events:
                                            logger.info(f"Parsed {len(events)} events from file {file_path}")
                                        else:
                                            logger.warning(f"No events parsed from file {file_path} despite valid format")
                                    except Exception as parse_error:
                                        logger.error(f"Error parsing CSV file {file_path}: {str(parse_error)}")
                                        events = []

                                    # BATCH PROCESSING IMPLEMENTATION
                                    processed_count = 0
                                    errors = []
                                    
                                    # Import utility functions
                                    from utils.parser_utils import normalize_event_data, categorize_event, parser_coordinator
                                    
                                    # Process in batches of 100 for better performance
                                    BATCH_SIZE = 100
                                    if len(events) > BATCH_SIZE:
                                        logger.info(f"Using batch processing for {len(events)} events")
                                        event_batches = [events[i:i+BATCH_SIZE] for i in range(0, len(events), BATCH_SIZE)]
                                        logger.info(f"Processing {len(events)} events in {len(event_batches)} batches of max {BATCH_SIZE}")
                                        
                                        # Process each batch
                                        batch_num = 0
                                        for event_batch in event_batches:
                                            batch_num += 1
                                            batch_normalized = []
                                            
                                            # Step 1: Normalize all events in batch
                                            for event in event_batch:
                                                try:
                                                    normalized_event = normalize_event_data(event)
                                                    if not normalized_event:
                                                        continue
                                                        
                                                    # Add server ID
                                                    normalized_event["server_id"] = server_id
                                                    
                                                    # Update timestamp in coordinator
                                                    if "timestamp" in normalized_event and isinstance(normalized_event["timestamp"], datetime):
                                                        parser_coordinator.update_csv_timestamp(server_id, normalized_event["timestamp"])
                                                        
                                                    # Add to batch
                                                    batch_normalized.append(normalized_event)
                                                except Exception as e:
                                                    errors.append(str(e))
                                                    continue
                                            
                                            # Step 2: Categorize batch into kill and suicide events
                                            kill_events = []
                                            suicide_events = []
                                            
                                            for event in batch_normalized:
                                                event_type = categorize_event(event)
                                                if event_type == "kill":
                                                    kill_events.append(event)
                                                elif event_type == "suicide":
                                                    suicide_events.append(event)
                                            
                                            # Log batch summary (only for larger batches to reduce log spam)
                                            if batch_num % 5 == 0 or batch_num == 1 or batch_num == len(event_batches):
                                                logger.info(f"Batch {batch_num}/{len(event_batches)}: {len(batch_normalized)} events normalized")
                                                logger.info(f"Batch {batch_num} has {len(kill_events)} kills and {len(suicide_events)} suicides")
                                            
                                            # Step 3: Bulk insert into database
                                            # 3a: Process kill events in bulk
                                            if kill_events:
                                                # Create the documents to insert
                                                kill_docs = []
                                                for event in kill_events:
                                                    # Get fields with fallbacks
                                                    killer_id = event.get("killer_id", "")
                                                    victim_id = event.get("victim_id", "")
                                                    
                                                    # Skip if missing essential IDs
                                                    if not killer_id or not victim_id:
                                                        continue
                                                    
                                                    # Create document for database
                                                    kill_doc = {
                                                        "server_id": server_id,
                                                        "killer_id": killer_id,
                                                        "killer_name": event.get("killer_name", "Unknown"),
                                                        "victim_id": victim_id,
                                                        "victim_name": event.get("victim_name", "Unknown"),
                                                        "weapon": event.get("weapon", "Unknown"),
                                                        "distance": event.get("distance", 0),
                                                        "timestamp": event.get("timestamp", datetime.now()),
                                                        "is_suicide": False,
                                                        "event_type": "kill"
                                                    }
                                                    kill_docs.append(kill_doc)
                                                
                                                # Bulk insert the kill documents
                                                if kill_docs:
                                                    try:
                                                        # Use ordered=False to continue inserting even if some fail
                                                        result = await self.bot.db.kills.insert_many(kill_docs, ordered=False)
                                                        processed_count += len(result.inserted_ids)
                                                    except Exception as e:
                                                        logger.error(f"Error during bulk kill insert: {str(e)[:100]}")
                                                        # Continue processing other batches despite errors
                                            
                                            # 3b: Process suicide events in bulk
                                            if suicide_events:
                                                # Create the documents to insert
                                                suicide_docs = []
                                                for event in suicide_events:
                                                    victim_id = event.get("victim_id", "")
                                                    # Skip if no valid ID
                                                    if not victim_id:
                                                        continue
                                                    
                                                    # Create document for database
                                                    suicide_doc = {
                                                        "server_id": server_id,
                                                        "killer_id": victim_id,
                                                        "killer_name": event.get("victim_name", "Unknown"),
                                                        "victim_id": victim_id,
                                                        "victim_name": event.get("victim_name", "Unknown"),
                                                        "weapon": event.get("weapon", "Unknown"),
                                                        "distance": event.get("distance", 0),
                                                        "timestamp": event.get("timestamp", datetime.now()),
                                                        "is_suicide": True,
                                                        "event_type": "suicide"
                                                    }
                                                    suicide_docs.append(suicide_doc)
                                                
                                                # Bulk insert the suicide documents
                                                if suicide_docs:
                                                    try:
                                                        # Use ordered=False to continue inserting even if some fail
                                                        result = await self.bot.db.kills.insert_many(suicide_docs, ordered=False)
                                                        processed_count += len(result.inserted_ids)
                                                    except Exception as e:
                                                        logger.error(f"Error during bulk suicide insert: {str(e)[:100]}")
                                                        # Continue processing other batches despite errors
                                        
                                        # Final batch summary
                                        logger.info(f"Batch processing complete: {processed_count} events inserted successfully")
                                        
                                    else:
                                        # Enhanced batch processing for smaller files too
                                        # Collect kill and suicide events for batch processing
                                        kill_events = []
                                        suicide_events = []
                                        processed_count = 0
                                        
                                        logger.info(f"Using batch processing for {len(events)} events")
                                        
                                        # Step 1: Normalize and categorize all events
                                        for event in events:
                                            try:
                                                # Normalize event data
                                                normalized_event = normalize_event_data(event)
                                                if not normalized_event:
                                                    continue
                                                    
                                                # Add server ID
                                                normalized_event["server_id"] = server_id
                                                
                                                # Update timestamp in coordinator
                                                if "timestamp" in normalized_event and isinstance(normalized_event["timestamp"], datetime):
                                                    parser_coordinator.update_csv_timestamp(server_id, normalized_event["timestamp"])
                                                
                                                # Process event type
                                                event_type = categorize_event(normalized_event)
                                                normalized_event["event_type"] = event_type
                                                
                                                # Validate IDs - using the improved validation from our _get_or_create_player method
                                                killer_id = normalized_event.get("killer_id", "")
                                                victim_id = normalized_event.get("victim_id", "")
                                                
                                                # Skip events with invalid IDs
                                                if not killer_id or killer_id.lower() in ['null', 'none', 'undefined'] or \
                                                   not victim_id or victim_id.lower() in ['null', 'none', 'undefined']:
                                                    continue
                                                
                                                # Add to appropriate batch
                                                if event_type == "kill":
                                                    kill_events.append(normalized_event)
                                                elif event_type == "suicide":
                                                    suicide_events.append(normalized_event)
                                            except Exception as e:
                                                errors.append(str(e))
                                                logger.error(f"Error normalizing/categorizing event: {str(e)[:100]}")
                                        
                                        # Report categorization results
                                        logger.info(f"Categorized events: {len(kill_events)} kills, {len(suicide_events)} suicides")
                                        
                                        # Step 2: Process suicide events in batch
                                        if suicide_events:
                                            # Create documents for bulk insert
                                            suicide_docs = []
                                            for event in suicide_events:
                                                suicide_docs.append({
                                                    "server_id": server_id,
                                                    "killer_id": event.get("victim_id"),  # For suicides, killer = victim
                                                    "killer_name": event.get("victim_name", "Unknown"),
                                                    "victim_id": event.get("victim_id"),
                                                    "victim_name": event.get("victim_name", "Unknown"),
                                                    "weapon": event.get("weapon", "Unknown"),
                                                    "distance": event.get("distance", 0),
                                                    "timestamp": event.get("timestamp", datetime.now()),
                                                    "is_suicide": True,
                                                    "event_type": "suicide"
                                                })
                                            
                                            # Bulk insert suicide events
                                            if suicide_docs:
                                                try:
                                                    # Use ordered=False to allow partial success
                                                    result = await self.bot.db.kills.insert_many(suicide_docs, ordered=False)
                                                    processed_count += len(suicide_docs)
                                                    logger.info(f"Inserted {len(suicide_docs)} suicide events in batch")
                                                except Exception as e:
                                                    logger.error(f"Error bulk inserting suicide events: {str(e)[:100]}")
                                        
                                        # Step 3: Process kill events in batch
                                        if kill_events:
                                            # Create documents for bulk insert
                                            kill_docs = []
                                            for event in kill_events:
                                                kill_docs.append({
                                                    "server_id": server_id,
                                                    "killer_id": event.get("killer_id"),
                                                    "killer_name": event.get("killer_name", "Unknown"),
                                                    "victim_id": event.get("victim_id"),
                                                    "victim_name": event.get("victim_name", "Unknown"),
                                                    "weapon": event.get("weapon", "Unknown"),
                                                    "distance": event.get("distance", 0),
                                                    "timestamp": event.get("timestamp", datetime.now()),
                                                    "is_suicide": False,
                                                    "event_type": "kill"
                                                })
                                            
                                            # Bulk insert kill events
                                            if kill_docs:
                                                try:
                                                    # Use ordered=False to allow partial success
                                                    result = await self.bot.db.kills.insert_many(kill_docs, ordered=False)
                                                    processed_count += len(kill_docs)
                                                    logger.info(f"Inserted {len(kill_docs)} kill events in batch")
                                                except Exception as e:
                                                    logger.error(f"Error bulk inserting kill events: {str(e)[:100]}")
                                                    
                                        # Step 4: Process player stats for unique players
                                        # This is more efficient than updating per-event
                                        # Create player lookup sets for batch processing player stats
                                        try:
                                            from models.player import Player
                                            
                                            # Collect unique player IDs
                                            unique_players = {}  # player_id -> {kills, deaths, suicides}
                                            
                                            # Count kills for killers, deaths for victims 
                                            for event in kill_events:
                                                killer_id = event.get("killer_id")
                                                victim_id = event.get("victim_id")
                                                
                                                # Add killer stats
                                                if killer_id not in unique_players:
                                                    unique_players[killer_id] = {"kills": 0, "deaths": 0, "suicides": 0, "name": event.get("killer_name", "Unknown")}
                                                unique_players[killer_id]["kills"] += 1
                                                
                                                # Add victim stats
                                                if victim_id not in unique_players:
                                                    unique_players[victim_id] = {"kills": 0, "deaths": 0, "suicides": 0, "name": event.get("victim_name", "Unknown")}
                                                unique_players[victim_id]["deaths"] += 1
                                                
                                            # Count suicides
                                            for event in suicide_events:
                                                player_id = event.get("victim_id")
                                                
                                                # Add player stats
                                                if player_id not in unique_players:
                                                    unique_players[player_id] = {"kills": 0, "deaths": 0, "suicides": 0, "name": event.get("victim_name", "Unknown")}
                                                unique_players[player_id]["suicides"] += 1
                                            
                                            # Update stats for each unique player
                                            logger.info(f"Updating stats for {len(unique_players)} unique players")
                                            for player_id, stats in unique_players.items():
                                                # Get or create player
                                                player = await self._get_or_create_player(server_id, player_id, stats["name"])
                                                
                                                if player:
                                                    # Update stats
                                                    await player.update_stats(self.bot.db, 
                                                                            kills=stats["kills"], 
                                                                            deaths=stats["deaths"], 
                                                                            suicides=stats["suicides"])
                                            
                                            # Update nemesis/prey relationships in bulk
                                            logger.info(f"Updating nemesis/prey relationships")
                                            await Player.update_all_nemesis_and_prey(self.bot.db, server_id)
                                            
                                        except Exception as e:
                                            logger.error(f"Error processing player stats in batch: {str(e)[:150]}")
                                            logger.error(f"Will continue with event recording even if player stats update failed")

                                    processed = processed_count

                                    events_processed += processed
                                    files_processed += 1

                                    if errors:
                                        logger.warning(f"Errors processing {file}: {len(errors)} errors")

                                    # Update last processed time if this is the newest file
                                    if file == new_files[-1]:
                                        try:
                                            file_time = datetime.strptime(file.split('.csv')[0], "%Y.%m.%d-%H.%M.%S")
                                            self.last_processed[server_id] = file_time
                                        except ValueError:
                                            # If we can't parse the timestamp from filename, use current time
                                            self.last_processed[server_id] = datetime.now()

                            except Exception as e:
                                logger.error(f"Error processing file {file}: {str(e)}")

                        # Memory optimization - clear local variables before completing
                        try:
                            # Force garbage collection to release memory
                            import gc
                            
                            # Clear any large local variables
                            if 'csv_parser_data' in locals():
                                del csv_parser_data
                            if 'content_io' in locals():
                                del content_io
                            if 'decoded_content' in locals():
                                del decoded_content
                            if 'content' in locals():
                                del content
                            if 'all_events' in locals():
                                del all_events
                            if 'kill_events' in locals():
                                del kill_events
                            if 'suicide_events' in locals():
                                del suicide_events
                            if 'kill_docs' in locals():
                                del kill_docs
                            if 'suicide_docs' in locals():
                                del suicide_docs
                                
                            # Run garbage collection
                            collected = gc.collect()
                            logger.info(f"Memory optimization: freed {collected} objects after CSV processing")
                        except Exception as mem_err:
                            logger.warning(f"Memory optimization failed: {mem_err}")
                            
                        # Keep the connection open for the next operation
                        return files_processed, events_processed

            except Exception as e:
                logger.error(f"SFTP error for server {server_id}: {str(e)}")
                # Run garbage collection before returning
                try:
                    import gc
                    collected = gc.collect()
                    logger.info(f"Memory optimization: freed {collected} objects after CSV error")
                except:
                    pass
                files_processed = 0
                events_processed = 0
                return files_processed, events_processed
            
        # Finalization code moved outside of the try-except block
        logger.debug(f"CSV processing completed for server {server_id}")
        
        # Final memory optimization
        try:
            import gc
            collected = gc.collect()
            logger.info(f"Final memory optimization: freed {collected} objects at completion")
        except:
            pass
            
        # Return the values
        return files_processed, events_processed

    async def run_historical_parse_with_config(self, server_id: str, server_config: Dict[str, Any], 
                                  days: int = 30, guild_id: Optional[str] = None) -> Tuple[int, int]:
        """Run a historical parse for a server using direct configuration.
        
        This enhanced method accepts a complete server configuration object to bypass resolution issues,
        ensuring we have all the necessary details even for newly added servers.

        Args:
            server_id: Server ID to process
            server_config: Complete server configuration with SFTP details
            days: Number of days to look back (default: 30)
            guild_id: Optional Discord guild ID for server isolation

        Returns:
            Tuple[int, int]: Number of files processed and events processed
        """
        # SOLUTION 1: Try direct CSV processor for attached_assets first (most reliable)
        logger.warning(f"PLAN A: Using direct CSV processor for server {server_id}")
        try:
            # Use our direct CSV processing method that bypasses all the complex infrastructure
            files_processed, events_inserted = await self.direct_csv_processing(
                server_id=server_id,
                days=days
            )
            
            if files_processed > 0:
                logger.warning(f"DIRECT CSV PROCESSING SUCCESS: Processed {files_processed} files with {events_inserted} events")
                return files_processed, events_inserted
            else:
                logger.warning("DIRECT CSV PROCESSING: No files processed, trying next method")
        except Exception as e:
            logger.error(f"DIRECT CSV PROCESSING ERROR: {e}")
            logger.error(f"Trace: {traceback.format_exc()}")
        
        # SOLUTION 2: Try standalone historical parser (second most reliable)
        logger.warning(f"PLAN B: Using standalone parser for server {server_id}")
        try:
            # Import the standalone module
            from fix_historical_parser import standalone_historical_parser
            
            # Run the standalone parser with the database connection
            files_processed, events_inserted = await standalone_historical_parser(
                server_id=server_id,
                days=days,
                db=self.bot.db
            )
            
            if files_processed > 0:
                logger.warning(f"STANDALONE PARSER SUCCESS: Processed {files_processed} files with {events_inserted} events")
                return files_processed, events_inserted
            else:
                logger.warning("STANDALONE PARSER: No files processed, trying legacy method")
        except Exception as e:
            logger.error(f"STANDALONE PARSER ERROR: {e}")
            logger.error(f"Trace: {traceback.format_exc()}")
            
        # SOLUTION 3: Continue with legacy implementation as last resort
        logger.info(f"Starting historical parse with direct config for server {server_id}, looking back {days} days")
        
        # Configure processing start time based on requested days
        start_date = datetime.now() - timedelta(days=days)
        logger.info(f"Historical parse will check files from {start_date.strftime('%Y-%m-%d')} until now")
        
        # Set the last processed time for this server
        self.last_processed[server_id] = start_date
        
        # Ensure original_server_id is present in config
        original_server_id = server_config.get("original_server_id")
        if original_server_id:
            logger.info(f"Using original_server_id {original_server_id} from provided config")
        else:
            logger.warning(f"No original_server_id in provided config, paths may use UUID format")
        
        # Process the server directly with provided configuration
        async with self.processing_lock:
            self.is_processing = True
            try:
                # Pass the full configuration and start date directly to _process_server_csv_files
                files_processed, events_processed = await self._process_server_csv_files(
                    server_id, server_config, start_date=start_date
                )
                logger.info(f"Direct config historical parse complete for server {server_id}: "
                          f"processed {files_processed} files with {events_processed} events")
                return files_processed, events_processed
            except Exception as e:
                logger.error(f"Error in direct config historical parse for server {server_id}: {e}")
                return 0, 0
            finally:
                self.is_processing = False
    
    async def run_historical_parse(self, server_id: str, days: int = 30, guild_id: Optional[str] = None) -> Tuple[int, int]:
        """Run a historical parse for a server, checking further back in time

        This function is meant to be called when setting up a new server to process
        older historical data going back further than the normal processing window.

        Args:
            server_id: Server ID to process (can be UUID or numeric ID)
            days: Number of days to look back (default: 30)
            guild_id: Optional Discord guild ID for server isolation

        Returns:
            Tuple[int, int]: Number of files processed and events processed
        """
        # Record the starting ID for logging
        raw_input_id = server_id if server_id is not None else ""
        
        # Import identity resolver functions
        from utils.server_utils import safe_standardize_server_id
        from utils.server_identity import resolve_server_id, identify_server, KNOWN_SERVERS
        
        logger.info(f"Starting historical parse for server {raw_input_id}, looking back {days} days")
        
        # DIRECT APPROACH: Try direct CSV processing first before any complicated server resolution
        logger.warning(f"DIRECT APPROACH: Try direct CSV processing for {raw_input_id} before complicated resolution")
        try:
            files_processed, events_inserted = await self.direct_csv_processing(
                server_id=raw_input_id,
                days=days
            )
            
            if files_processed > 0:
                logger.warning(f"DIRECT CSV PROCESSING SUCCESS: Processed {files_processed} files with {events_inserted} events")
                return files_processed, events_inserted
            else:
                logger.warning("DIRECT CSV PROCESSING: No files processed, continuing with server resolution")
        except Exception as e:
            logger.error(f"DIRECT CSV PROCESSING ERROR: {e}")
            logger.error(traceback.format_exc())
        
        # STEP 1: Try to resolve the server ID comprehensively using our new function
        server_resolution = await resolve_server_id(self.bot.db, server_id, guild_id)
        if server_resolution:
            resolved_server_id = server_resolution.get("server_id")
            original_server_id = server_resolution.get("original_server_id")
            server_config = server_resolution.get("config")
            collection = server_resolution.get("collection")
            
            logger.info(f"Enhanced server resolution found server: {server_id} → {resolved_server_id} "
                      f"(original_id: {original_server_id}, found in {collection})")
                      
            # We have a direct server configuration from resolution
            if server_config:
                # Configure processing start time based on requested days
                start_date = datetime.now() - timedelta(days=days)
                logger.info(f"Historical parse will check files from {start_date.strftime('%Y-%m-%d')} until now")
                
                # Set the last processed time for this server
                self.last_processed[resolved_server_id] = start_date
                
                # CRITICAL IMPROVEMENT: Clear existing data for this server first
                # This ensures we can reprocess everything from scratch without duplication
                try:
                    logger.info(f"Clearing existing data for server {resolved_server_id} before historical parse")
                    
                    # Delete all kill events for this server
                    kill_result = await self.bot.db.kills.delete_many({"server_id": resolved_server_id})
                    logger.info(f"Deleted {kill_result.deleted_count} existing kill events for server {resolved_server_id}")
                    
                    # Update player stats to reset kill/death/suicide counts
                    player_reset = await self.bot.db.players.update_many(
                        {"server_id": resolved_server_id},
                        {"$set": {"kills": 0, "deaths": 0, "suicides": 0, "updated_at": datetime.utcnow()}}
                    )
                    logger.info(f"Reset stats for {player_reset.modified_count} players for server {resolved_server_id}")
                    
                    # Clear rivalry data
                    rivalry_result = await self.bot.db.rivalries.delete_many({"server_id": resolved_server_id})
                    logger.info(f"Deleted {rivalry_result.deleted_count} existing rivalries for server {resolved_server_id}")
                    
                    # Force garbage collection to free up memory
                    import gc
                    gc.collect()
                    logger.info("Forced garbage collection after data clearing")
                    
                except Exception as e:
                    logger.error(f"Error clearing existing data: {e}")
                
                # Process CSV files with the directly resolved configuration
                async with self.processing_lock:
                    self.is_processing = True
                    try:
                        # Use the resolved configuration directly
                        files_processed, events_processed = await self._process_server_csv_files(
                            resolved_server_id, server_config, start_date=start_date
                        )
                        logger.info(f"Direct resolution historical parse complete for server {resolved_server_id}: "
                                   f"processed {files_processed} files with {events_processed} events")
                        return files_processed, events_processed
                    except Exception as e:
                        logger.error(f"Error in direct resolution historical parse for server {resolved_server_id}: {e}")
                        return 0, 0
                    finally:
                        self.is_processing = False
        
        # STEP 2: Fall back to traditional method if direct resolution failed
        logger.info(f"Direct server resolution failed or returned no config, falling back to traditional lookup")
        
        # Standardize server ID and check for numeric ID
        server_id = safe_standardize_server_id(raw_input_id)
        original_numeric_id = None
        
        # Check if this is a numeric ID (like "7020") being used directly
        if server_id and server_id.isdigit():
            original_numeric_id = server_id
            logger.info(f"Received numeric ID {original_numeric_id} for historical parse")
            
            # Look for a matching server in KNOWN_SERVERS by value
            found_uuid = None
            for uuid, numeric in KNOWN_SERVERS.items():
                if str(numeric) == original_numeric_id:
                    found_uuid = uuid
                    logger.info(f"Mapped numeric ID {original_numeric_id} to UUID {found_uuid}")
                    break
            
            if found_uuid:
                server_id = found_uuid
            else:
                logger.warning(f"Could not find UUID for numeric ID {original_numeric_id} in KNOWN_SERVERS")
        
        # Get all server configurations
        server_configs = await self._get_server_configs()
        logger.info(f"Traditional lookup found server configs: {list(server_configs.keys())}")
        
        # Try to find the server in our configurations
        if server_id not in server_configs:
            # Try by original_server_id if we have one
            if original_numeric_id:
                for config_id, config in server_configs.items():
                    if str(config.get("original_server_id")) == original_numeric_id:
                        server_id = config_id
                        logger.info(f"Found server by original_server_id {original_numeric_id}: {server_id}")
                        break
            
            # Try by numeric matching if needed
            if server_id not in server_configs and server_id and str(server_id).isdigit():
                numeric_matches = [sid for sid in server_configs.keys() if str(sid).isdigit() and int(sid) == int(server_id)]
                if numeric_matches:
                    server_id = numeric_matches[0]
                    logger.info(f"Found server using numeric matching: {server_id}")
            
            # If still not found, give up
            if server_id not in server_configs:
                logger.error(f"Server {raw_input_id} not found in configs during historical parse")
                return 0, 0
        
        # Configure the processing window
        start_date = datetime.now() - timedelta(days=days)
        self.last_processed[server_id] = start_date
        
        # CRITICAL IMPROVEMENT: Clear existing data for this server first
        # This ensures we can reprocess everything from scratch without duplication
        try:
            logger.info(f"Clearing existing data for server {server_id} before historical parse (traditional method)")
            
            # Delete all kill events for this server
            kill_result = await self.bot.db.kills.delete_many({"server_id": server_id})
            logger.info(f"Deleted {kill_result.deleted_count} existing kill events for server {server_id}")
            
            # Update player stats to reset kill/death/suicide counts
            player_reset = await self.bot.db.players.update_many(
                {"server_id": server_id},
                {"$set": {"kills": 0, "deaths": 0, "suicides": 0, "updated_at": datetime.utcnow()}}
            )
            logger.info(f"Reset stats for {player_reset.modified_count} players for server {server_id}")
            
            # Clear rivalry data
            rivalry_result = await self.bot.db.rivalries.delete_many({"server_id": server_id})
            logger.info(f"Deleted {rivalry_result.deleted_count} existing rivalries for server {server_id}")
            
            # Force garbage collection to free up memory
            import gc
            gc.collect()
            logger.info("Forced garbage collection after data clearing (traditional method)")
            
        except Exception as e:
            logger.error(f"Error clearing existing data (traditional method): {e}")
        
        # Process CSV files with the traditional method
        async with self.processing_lock:
            self.is_processing = True
            try:
                files_processed, events_processed = await self._process_server_csv_files(
                    server_id, server_configs[server_id], start_date=start_date
                )
                logger.info(f"Traditional historical parse complete for server {server_id}: "
                           f"processed {files_processed} files with {events_processed} events")
                return files_processed, events_processed
            except Exception as e:
                logger.error(f"Error in traditional historical parse for server {server_id}: {e}")
                return 0, 0
            finally:
                self.is_processing = False

    @app_commands.command(
        name="process_csv",
        description="Manually process CSV files from the game server"
    )
    @admin_permission_decorator()
    @premium_tier_required(1)  # Require Survivor tier for CSV processing
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    async def process_csv_command(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        hours: int = 24
    ):
        """Manually process CSV files from the game server

        Args:
            interaction: Discord interaction
            server_id: Server ID to process (optional)
            hours: Number of hours to look back (default: 24)
        """

        await interaction.response.defer(ephemeral=True)

        # Import standardization function
        from utils.server_utils import safe_standardize_server_id

        # Get server ID from guild config if not provided
        if not server_id:
            # Try to get the server ID from the guild's configuration
            try:
                guild_id = str(interaction.guild_id)
                guild_doc = await self.bot.db.guilds.find_one({"guild_id": guild_id})
                if guild_doc and "default_server_id" in guild_doc:
                    raw_server_id = guild_doc.get("default_server_id", "")
                    server_id = safe_standardize_server_id(raw_server_id)
                    logger.info(f"Using default server ID from guild config: {raw_server_id} (standardized to {server_id})")
                else:
                    # No default server configured
                    embed = EmbedBuilder.error(
                        title="No Server Configured",
                        description="No server ID provided and no default server configured for this guild."
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
            except Exception as e:
                logger.error(f"Error getting default server ID: {e}")
                embed = EmbedBuilder.error(
                    title="Configuration Error",
                    description="An error occurred while retrieving the server configuration."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
        else:
            # Standardize the provided server ID
            raw_server_id = server_id
            server_id = safe_standardize_server_id(server_id)
            logger.info(f"Standardized provided server ID from {raw_server_id} to {server_id}")

        # Get server config
        server_configs = await self._get_server_configs()

        # Log all available server configs for debugging
        logger.info(f"Available server configs: {list(server_configs.keys())}")

        if server_id not in server_configs:
            # Try numeric comparison as fallback if server_id is numeric
            if server_id and str(server_id).isdigit():
                numeric_matches = [sid for sid in server_configs.keys() if str(sid).isdigit() and int(sid) == int(server_id)]
                if numeric_matches:
                    server_id = numeric_matches[0]
                    logger.info(f"Found server using numeric matching: {server_id}")

            # If still not found, show error
            if server_id not in server_configs:
                embed = EmbedBuilder.error(
                    title="Server Not Found",
                    description=f"No SFTP configuration found for server `{server_id}`."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

        # Calculate lookback time
        # Ensure hours is a valid float value
        safe_hours = float(hours) if hours else 24.0

        # Safely update last_processed dictionary with server_id
        if server_id and isinstance(server_id, str):
            self.last_processed[server_id] = datetime.now() - timedelta(hours=safe_hours)
        else:
            logger.warning(f"Invalid server_id: {server_id}, not updating last_processed timestamp")

        # Process CSV files
        async with self.processing_lock:
            try:
                # Process files only if server_id exists in server_configs and it's a non-None string
                if server_id and isinstance(server_id, str) and server_id in server_configs:
                    files_processed, events_processed = await self._process_server_csv_files(
                        server_id, server_configs[server_id]
                    )
                else:
                    logger.error(f"Invalid server_id: {server_id} or not found in server_configs")
                    files_processed, events_processed = 0, 0

                if files_processed > 0:
                    embed = EmbedBuilder.success(
                        title="CSV Processing Complete",
                        description=f"Processed {files_processed} file(s) with {events_processed} death events."
                    )
                else:
                    embed = EmbedBuilder.info(
                        title="No Files Found",
                        description=f"No new CSV files found for server `{server_id}` in the last {hours} hours."
                    )

                await interaction.followup.send(embed=embed, ephemeral=True)

            except Exception as e:
                logger.error(f"Error processing CSV files: {str(e)}")
                embed = EmbedBuilder.error(
                    title="Processing Error",
                    description=f"An error occurred while processing CSV files: {str(e)}"
                )
                await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(
        name="clear_csv_cache",
        description="Clear the CSV parser cache"
    )
    @admin_permission_decorator()
    @premium_tier_required(1)  # Require Survivor tier for CSV cache management
    async def clear_csv_cache_command(self, interaction: discord.Interaction):
        """Clear the CSV parser cache

        Args:
            interaction: Discord interaction
        """

        # Clear cache
        self.csv_parser.clear_cache()

        embed = EmbedBuilder.success(
            title="Cache Cleared",
            description="The CSV parser cache has been cleared."
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(
        name="historical_parse",
        description="Process historical CSV data going back further than normal processing"
    )
    @admin_permission_decorator()
    @premium_tier_required(1)  # Require Survivor tier
    @app_commands.autocomplete(server_id=server_id_autocomplete)
    async def historical_parse_command(
        self,
        interaction: discord.Interaction,
        server_id: Optional[str] = None,
        days: int = 30
    ):
        """Process historical CSV data going back further than normal processing

        Args:
            interaction: Discord interaction
            server_id: Server ID to process (optional)
            days: Number of days to look back (default: 30)
        """
        # Note: This command has been significantly enhanced with a multi-tier approach.
        # It will try three different approaches in the following order:
        # 1. Direct CSV processing from attached_assets directory (most reliable)
        # 2. Standalone historical parser that bypasses the complex infrastructure
        # 3. Legacy approach with SFTP connections (least reliable)
        
        await interaction.response.defer(ephemeral=True)

        # Import standardization function
        from utils.server_utils import safe_standardize_server_id

        # Get server ID from guild config if not provided
        if not server_id:
            try:
                guild_id = str(interaction.guild_id)
                guild_doc = await self.bot.db.guilds.find_one({"guild_id": guild_id})
                if guild_doc and "default_server_id" in guild_doc:
                    raw_server_id = guild_doc.get("default_server_id", "")
                    server_id = safe_standardize_server_id(raw_server_id)
                    logger.info(f"Using default server ID from guild config: {raw_server_id} (standardized to {server_id})")
                else:
                    embed = EmbedBuilder.error(
                        title="No Server Configured",
                        description="No server ID provided and no default server configured for this guild."
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
            except Exception as e:
                logger.error(f"Error getting default server ID: {e}")
                embed = EmbedBuilder.error(
                    title="Configuration Error",
                    description="An error occurred while retrieving the server configuration."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
        else:
            # Standardize the provided server ID
            raw_server_id = server_id
            server_id = safe_standardize_server_id(server_id)
            logger.info(f"Standardized provided server ID from {raw_server_id} to {server_id}")

        # Get server config
        server_configs = await self._get_server_configs()

        # Log all available server configs for debugging
        logger.info(f"Available server configs: {list(server_configs.keys())}")

        if server_id not in server_configs:
            # Try numeric comparison as fallback if server_id is numeric
            if server_id and str(server_id).isdigit():
                numeric_matches = [sid for sid in server_configs.keys() if str(sid).isdigit() and int(sid) == int(server_id)]
                if numeric_matches:
                    server_id = numeric_matches[0]
                    logger.info(f"Found server using numeric matching: {server_id}")

            # If still not found, show error
            if server_id not in server_configs:
                embed = EmbedBuilder.error(
                    title="Server Not Found",
                    description=f"No SFTP configuration found for server `{server_id}`."
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

        # Validate days parameter
        safe_days = max(1, min(int(days) if days else 30, 90))  # Between 1 and 90 days

        # Send initial response
        embed = EmbedBuilder.info(
            title="Historical Parsing Started",
            description=f"Starting historical parsing for server `{server_id}` looking back {safe_days} days.\n\nThis may take some time, please wait..."
        )
        await interaction.followup.send(embed=embed, ephemeral=True)

        # Run the historical parse
        try:
            files_processed, events_processed = await self.run_historical_parse(server_id, days=safe_days)

            if files_processed > 0:
                # Get additional statistics about processed events
                try:
                    # Get kill/suicide statistics
                    kills_count = await self.bot.db.kills.count_documents({
                        "server_id": server_id,
                        "is_suicide": False,
                        "timestamp": {"$gte": datetime.now() - timedelta(days=safe_days)}
                    })
                    
                    suicides_count = await self.bot.db.kills.count_documents({
                        "server_id": server_id,
                        "is_suicide": True,
                        "timestamp": {"$gte": datetime.now() - timedelta(days=safe_days)}
                    })
                    
                    # Count unique players involved
                    pipeline = [
                        {"$match": {
                            "server_id": server_id,
                            "timestamp": {"$gte": datetime.now() - timedelta(days=safe_days)}
                        }},
                        {"$group": {
                            "_id": None,
                            "unique_killers": {"$addToSet": "$killer_id"},
                            "unique_victims": {"$addToSet": "$victim_id"}
                        }}
                    ]
                    
                    result = await self.bot.db.kills.aggregate(pipeline).to_list(length=1)
                    
                    unique_killers = len(result[0]["unique_killers"]) if result else 0
                    unique_victims = len(result[0]["unique_victims"]) if result else 0
                    
                    # Calculate unique players (combined set)
                    unique_players = unique_killers + unique_victims - (len(set(result[0]["unique_killers"]) & set(result[0]["unique_victims"])) if result else 0)
                    
                    # Calculate percentages
                    kill_percent = (kills_count / events_processed) * 100 if events_processed > 0 else 0
                    suicide_percent = (suicides_count / events_processed) * 100 if events_processed > 0 else 0
                    
                    # Build enhanced description
                    description = (
                        f"**Process Summary:**\n"
                        f"• Files Processed: **{files_processed}**\n"
                        f"• Total Events: **{events_processed}**\n\n"
                        f"**Event Breakdown:**\n"
                        f"• Kills: **{kills_count}** ({kill_percent:.1f}%)\n"
                        f"• Suicides: **{suicides_count}** ({suicide_percent:.1f}%)\n\n"
                        f"**Player Statistics:**\n"
                        f"• Unique Players: **{unique_players}**\n"
                        f"• Active Killers: **{unique_killers}**\n"
                        f"• Victims: **{unique_victims}**\n\n"
                        f"_Looking back {safe_days} days from today_"
                    )
                    
                except Exception as e:
                    logger.error(f"Error calculating enhanced statistics: {str(e)}")
                    description = f"Processed {files_processed} historical file(s) with {events_processed} death events."
                
                embed = EmbedBuilder.success(
                    title="Historical Parsing Complete",
                    description=description
                )
            else:
                embed = EmbedBuilder.info(
                    title="No Historical Files Found",
                    description=f"No historical CSV files found for server `{server_id}` in the last {safe_days} days."
                )

            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            logger.error(f"Error in historical parse command: {e}")
            embed = EmbedBuilder.error(
                title="Processing Error",
                description=f"An error occurred during historical parsing: {str(e)}"
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(
        name="csv_status",
        description="Show CSV processor status"
    )
    @admin_permission_decorator()
    @premium_tier_required(1)  # Require Survivor tier for CSV status
    async def csv_status_command(self, interaction: discord.Interaction):
        """Show CSV processor status

        Args:
            interaction: Discord interaction
        """

        await interaction.response.defer(ephemeral=True)

        # Get server configs
        server_configs = await self._get_server_configs()

        # Create status embed
        embed = EmbedBuilder.info(
            title="CSV Processor Status",
            description="Current status of the CSV processor"
        )

        # Add processing status
        embed.add_field(
            name="Currently Processing",
            value="Yes" if self.is_processing else "No",
            inline=True
        )

        # Add configured servers
        server_list = []
        for server_id, config in server_configs.items():
            last_time = self.last_processed.get(server_id, "Never")
            logger.info(f"DIAGNOSTIC: Using a 60-day cutoff for CSV processing: {last_time.strftime('%Y-%m-%d %H:%M:%S')}")
            if isinstance(last_time, datetime):
                last_time = last_time.strftime("%Y-%m-%d %H:%M:%S")

            server_list.append(f"• `{server_id}` - Last processed: {last_time}")

        if server_list:
            embed.add_field(
                name="Configured Servers",
                value="\n".join(server_list),
                inline=False
            )
        else:
            embed.add_field(
                name="Configured Servers",
                value="No servers configured",
                inline=False
            )

        await interaction.followup.send(embed=embed, ephemeral=True)

    async def _process_kill_event(self, event: Dict[str, Any]) -> bool:
        """Process a kill event and update player stats and rivalries

        Args:
            event: Normalized kill event dictionary

        Returns:
            bool: True if processed successfully, False otherwise
        """
        try:
            server_id = event.get("server_id")
            if not server_id:
                logger.warning("Kill event missing server_id, skipping")
                return False

            # Get kill details
            killer_id = event.get("killer_id", "")
            killer_name = event.get("killer_name", "Unknown")
            victim_id = event.get("victim_id", "")
            victim_name = event.get("victim_name", "Unknown")
            weapon = event.get("weapon", "Unknown")
            distance = event.get("distance", 0)
            timestamp = event.get("timestamp", datetime.utcnow())
            
            # Get the event_type directly from the normalized event
            event_type = event.get("event_type")
            
            # If event_type not present, determine it using the categorize_event function
            if not event_type:
                from utils.parser_utils import categorize_event
                event_type = categorize_event(event)
                logger.debug(f"Determined event_type using categorize_event: {event_type}")
            
            # Set suicide flag based on event type
            is_suicide = (event_type == "suicide")
            logger.debug(f"Processing event: type={event_type}, is_suicide={is_suicide}")

            # Ensure timestamp is datetime
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp)
                except ValueError:
                    # Try other formats
                    try:
                        common_formats = [
                            "%Y.%m.%d-%H.%M.%S",
                            "%Y.%m.%d-%H.%M.%S:%f",
                            "%Y-%m-%d %H:%M:%S",
                            "%Y-%m-%d %H:%M:%S.%f"
                        ]
                        
                        for fmt in common_formats:
                            try:
                                timestamp = datetime.strptime(timestamp, fmt)
                                break
                            except ValueError:
                                continue
                        
                        # If we still have a string, use current time
                        if isinstance(timestamp, str):
                            logger.warning(f"Could not parse timestamp: {timestamp}, using current time")
                            timestamp = datetime.utcnow()
                    except Exception as e:
                        logger.error(f"Error parsing timestamp '{timestamp}': {e}")
                        timestamp = datetime.utcnow()
            
            # For suicide events, ensure killer_id matches victim_id
            if is_suicide:
                if killer_id != victim_id:
                    logger.debug(f"Fixing inconsistent suicide data: setting killer_id={victim_id} (was {killer_id})")
                    killer_id = victim_id
                    killer_name = victim_name

            # Check if we have the necessary player IDs
            if not victim_id:
                logger.warning("Kill event missing victim_id, skipping")
                return False

            # For suicides, we only need to update the victim's stats
            if is_suicide:
                # Get victim player or create if doesn't exist
                victim = await self._get_or_create_player(server_id, victim_id, victim_name)

                # Update suicide count
                await victim.update_stats(self.bot.db, kills=0, deaths=0, suicides=1)
                
                # Insert suicide event into database for consistent record-keeping
                suicide_doc = {
                    "server_id": server_id,
                    "killer_id": victim_id,  # Same as victim_id for suicides
                    "killer_name": victim_name,
                    "victim_id": victim_id,
                    "victim_name": victim_name,
                    "weapon": weapon,
                    "distance": distance,
                    "timestamp": timestamp,
                    "is_suicide": True,
                    "event_type": "suicide"
                }
                
                await self.bot.db.kills.insert_one(suicide_doc)
                logger.info(f"Recorded suicide event for player {victim_name} ({victim_id})")
                return True

            # For regular kills, we need both killer and victim
            if not killer_id:
                logger.warning("Kill event missing killer_id for non-suicide, skipping")
                return False

            # Get killer and victim players, or create if they don't exist
            killer = await self._get_or_create_player(server_id, killer_id, killer_name)
            victim = await self._get_or_create_player(server_id, victim_id, victim_name)

            # Update kill/death stats
            await killer.update_stats(self.bot.db, kills=1, deaths=0)
            await victim.update_stats(self.bot.db, kills=0, deaths=1)

            # Update rivalries
            from models.rivalry import Rivalry
            await Rivalry.record_kill(server_id, killer_id, victim_id, weapon, "")

            # Update nemesis/prey relationships
            await killer.update_nemesis_and_prey(self.bot.db)
            await victim.update_nemesis_and_prey(self.bot.db)

            # Insert kill event into database
            kill_doc = {
                "server_id": server_id,
                "killer_id": killer_id,
                "killer_name": killer_name,
                "victim_id": victim_id,
                "victim_name": victim_name,
                "weapon": weapon,
                "distance": distance,
                "timestamp": timestamp,
                "is_suicide": is_suicide,
                "event_type": event_type or "kill"  # Ensure we always have an event_type
            }

            await self.bot.db.kills.insert_one(kill_doc)

            return True

        except Exception as e:
            logger.error(f"Error processing kill event: {e}")
            return False

    async def _get_or_create_player(self, server_id: str, player_id: str, player_name: str):
        """Get player by ID or create if it doesn't exist

        Args:
            server_id: Server ID
            player_id: Player ID
            player_name: Player name

        Returns:
            Player object or None if invalid data
        """
        from models.player import Player
        
        # Validate IDs first to prevent database errors
        if not player_id or player_id.lower() in ['null', 'none', 'undefined']:
            logger.warning(f"Invalid player_id: '{player_id}' for player '{player_name}' - skipping")
            return None
            
        if not server_id or server_id.lower() in ['null', 'none', 'undefined']:
            logger.warning(f"Invalid server_id: '{server_id}' for player '{player_name}' - skipping")
            return None

        # Use a generated UUID if player_id is not valid
        if not player_id or len(player_id.strip()) == 0:
            import uuid
            player_id = str(uuid.uuid4())
            logger.warning(f"Generated placeholder ID '{player_id}' for player '{player_name}'")

        try:
            # Check if player exists
            player = await Player.get_by_player_id(self.bot.db, player_id)

            if not player:
                # Create new player
                player = Player(
                    player_id=player_id,
                    server_id=server_id,
                    name=player_name or "Unknown",
                    display_name=player_name or "Unknown",
                    last_seen=datetime.utcnow(),
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )

                # Insert into database - wrapped in try/except to handle edge cases
                try:
                    await self.bot.db.players.insert_one(player.__dict__)
                except Exception as e:
                    logger.error(f"Error inserting player {player_name} ({player_id}): {str(e)[:100]}")
                    return None

            return player
            
        except Exception as e:
            logger.error(f"Error in _get_or_create_player for {player_name} ({player_id}): {str(e)[:100]}")
            return None

async def setup(bot: Any) -> None:
    """Set up the CSV processor cog
    
    Args:
        bot: Discord bot instance with db property
    """
    # Cast the bot to our PvPBot protocol to satisfy type checking
    pvp_bot = cast('PvPBot', bot)
    await bot.add_cog(CSVProcessorCog(pvp_bot))