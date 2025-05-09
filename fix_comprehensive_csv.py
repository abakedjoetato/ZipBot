"""
Comprehensive CSV Parsing Fix

This script fixes multiple issues with CSV parsing in the ToT PvP Statistics Bot:
1. Improves delimiter detection with priority for semicolons
2. Enhances timestamp parsing with additional formats
3. Updates file pattern recognition to be more inclusive
4. Adds better diagnostic logging for parse failures
"""

import asyncio
import logging
import os
import re
import shutil
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("csv_fix.log", mode="w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ComprehensiveCSVFix:
    """Apply comprehensive fixes to the CSV parsing system"""
    
    def __init__(self):
        self.backup_dir = "backups_" + datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(self.backup_dir, exist_ok=True)
        self.files_to_fix = [
            "utils/csv_parser.py",
            "utils/sftp.py",
            "cogs/csv_processor.py"
        ]
        
    async def run(self):
        """Run all fixes in sequence"""
        try:
            logger.info("Starting comprehensive CSV fix")
            
            # Step 1: Create backups
            await self.create_backups()
            
            # Step 2: Fix CSV parser
            await self.fix_csv_parser()
            
            # Step 3: Fix SFTP file pattern matching
            await self.fix_sftp_pattern_matching()
            
            # Step 4: Fix CSV processor
            await self.fix_csv_processor()
            
            # Step 5: Test with sample files
            success = await self.test_fixes()
            
            if success:
                logger.info("✅ All fixes successfully applied and tested!")
                try:
                    await self.post_success_message()
                except Exception as e:
                    logger.warning(f"Could not post success message: {e}")
            else:
                logger.error("❌ Fix testing failed - restoring backups")
                await self.restore_backups()
        except Exception as e:
            logger.error(f"Error during CSV fix: {e}")
            import traceback
            traceback.print_exc()
            
            try:
                await self.restore_backups()
            except Exception as restore_error:
                logger.error(f"Failed to restore backups: {restore_error}")
    
    async def create_backups(self):
        """Create backups of all files to be modified"""
        logger.info("Creating backups of files to be modified")
        
        for file_path in self.files_to_fix:
            if os.path.exists(file_path):
                backup_path = os.path.join(self.backup_dir, os.path.basename(file_path))
                shutil.copy2(file_path, backup_path)
                logger.info(f"Created backup of {file_path} to {backup_path}")
            else:
                logger.warning(f"File {file_path} does not exist, skipping backup")
    
    async def restore_backups(self):
        """Restore backups if something goes wrong"""
        logger.info("Restoring backups")
        
        for file_path in self.files_to_fix:
            backup_path = os.path.join(self.backup_dir, os.path.basename(file_path))
            if os.path.exists(backup_path):
                shutil.copy2(backup_path, file_path)
                logger.info(f"Restored {file_path} from backup")
            else:
                logger.warning(f"No backup found for {file_path}")
    
    async def fix_csv_parser(self):
        """Fix issues in the CSV parser class"""
        file_path = "utils/csv_parser.py"
        logger.info(f"Fixing {file_path}")
        
        if not os.path.exists(file_path):
            logger.error(f"File {file_path} does not exist")
            return
        
        # Read the file
        with open(file_path, "r") as f:
            content = f.read()
        
        # Fix 1: Improve delimiter detection with priority for semicolons
        delimiter_detection_pattern = r"# Count occurrences of potential delimiters.*?delimiters\s*=\s*\{.*?\}"
        delimiter_detection_replacement = """# Count occurrences of potential delimiters
        delimiters = {';': 0, ',': 0, '\\t': 0}
        # Prioritize semicolons as they're commonly used in game log formats
        for d in delimiters:
            delimiters[d] = file_content.count(d)
        
        # Add extra weight to semicolons to handle mixed format files better
        if delimiters.get(';', 0) > 0:
            delimiters[';'] *= 1.5  # Give semicolons a 50% bonus in detection"""
        
        # Use regex to find and replace the delimiter detection code
        content = re.sub(delimiter_detection_pattern, delimiter_detection_replacement, content, flags=re.DOTALL)
        
        # Fix 2: Enhance timestamp parsing with additional formats
        timestamp_formats_pattern = r"alternative_formats\s*=\s*\[.*?\]"
        timestamp_formats_replacement = """alternative_formats = [
                            "%Y.%m.%d-%H.%M.%S",      # 2025.05.09-11.58.37 (primary format)
                            "%Y.%m.%d-%H:%M:%S",      # 2025.05.09-11:58:37 (variant with colons)
                            "%Y.%m.%d %H.%M.%S",      # 2025.05.09 11.58.37 (space instead of dash)
                            "%Y.%m.%d %H:%M:%S",      # 2025.05.09 11:58:37
                            "%Y-%m-%d-%H.%M.%S",      # 2025-05-09-11.58.37
                            "%Y-%m-%d %H:%M:%S",      # 2025-05-09 11:58:37
                            "%Y/%m/%d %H:%M:%S",      # 2025/05/09 11:58:37
                            "%d.%m.%Y-%H.%M.%S",      # 09.05.2025-11.58.37
                            "%d.%m.%Y %H:%M:%S",      # 09.05.2025 11:58:37
                            "%d-%m-%Y %H:%M:%S"       # 09-05-2025 11:58:37
                        ]"""
        
        # Use regex to find and replace the timestamp formats
        content = re.sub(timestamp_formats_pattern, timestamp_formats_replacement, content, flags=re.DOTALL)
        
        # Fix 3: Improve error handling and logging for failed timestamp parsing
        timestamp_error_pattern = r"if not parsed:.*?continue"
        timestamp_error_replacement = """if not parsed:
                                # Log specific details about the failed parse attempt
                                logger.warning(f"Failed to parse timestamp: '{timestamp_str}' - keeping as string")
                                # Keep the original string value instead of failing completely
                                event[self.datetime_column] = timestamp_str
                                # Add a flag indicating this event has an unparsed timestamp
                                event['_timestamp_parse_failed'] = True
                                # Continue processing - don't skip the whole row"""
        
        # Use regex to find and replace the timestamp error handling
        content = re.sub(timestamp_error_pattern, timestamp_error_replacement, content, flags=re.DOTALL)
        
        # Fix 4: Add more robust handling of row data even with missing fields
        row_validation_pattern = r"if row is None or len\(row\) < 6:.*?continue"
        row_validation_replacement = """if row is None:
                    logger.debug(f"Skipping empty row at line {current_line}")
                    continue
                    
                # More permissive handling of rows with insufficient fields
                if len(row) < 6:  # Minimum required fields for a kill event
                    logger.warning(f"Row {current_line} has insufficient fields ({len(row)} < 6): {row}")
                    # Try to extract whatever data we can anyway
                    if len(row) >= 3:
                        logger.debug(f"Attempting partial extraction from incomplete row: {row}")
                    else:
                        continue"""
        
        # Use regex to find and replace the row validation code
        content = re.sub(row_validation_pattern, row_validation_replacement, content, flags=re.DOTALL)
        
        # Write the updated content
        with open(file_path, "w") as f:
            f.write(content)
        
        logger.info(f"Fixed {file_path}")
    
    async def fix_sftp_pattern_matching(self):
        """Fix issues in the SFTP file pattern matching"""
        file_path = "utils/sftp.py"
        logger.info(f"Fixing {file_path}")
        
        if not os.path.exists(file_path):
            logger.error(f"File {file_path} does not exist")
            return
        
        # Read the file
        with open(file_path, "r") as f:
            content = f.read()
        
        # Fix 1: Improve CSV pattern matching to be more inclusive
        csv_patterns_pattern = r"csv_patterns\s*=\s*\[.*?\]"
        csv_patterns_replacement = r"""csv_patterns = [
            # Primary pattern - matches YYYY.MM.DD-HH.MM.SS.csv (Emeralds Killfeed standard)
            r'\\d{4}\\.\\d{2}\\.\\d{2}-\\d{2}\\.\\d{2}\\.\\d{2}\\.csv$',
            
            # Alternative patterns - match various date formats
            r'\\d{4}\\.\\d{2}\\.\\d{2}.*?\\.csv$',    # YYYY.MM.DD*.csv (any time format)
            r'\\d{4}-\\d{2}-\\d{2}.*?\\.csv$',        # YYYY-MM-DD*.csv
            r'\\d{2}\\.\\d{2}\\.\\d{4}.*?\\.csv$',    # DD.MM.YYYY*.csv
            r'\\d{2}-\\d{2}-\\d{4}.*?\\.csv$',        # DD-MM-YYYY*.csv
            r'.*?death.*?\\.csv$',                    # Any filename with 'death' in it
            r'.*?kill.*?\\.csv$',                     # Any filename with 'kill' in it
            r'.*?pvp.*?\\.csv$',                      # Any filename with 'pvp' in it
            r'.*?player.*?\\.csv$'                    # Any filename with 'player' in it
        ]"""
        
        # Use regex to find and replace the CSV patterns
        content = re.sub(csv_patterns_pattern, csv_patterns_replacement, content, flags=re.DOTALL)
        
        # Fix 2: Enhance the file finding logic to be more thorough
        find_files_pattern = r"async def find_files_by_pattern\(.*?return (?:matched_files|files)"
        
        # Read the method content before replacing
        method_match = re.search(find_files_pattern, content, re.DOTALL)
        if method_match:
            method_content = method_match.group(0)
            
            # Add exponential backoff retry logic
            if "max_retries" not in method_content:
                # Find method signature to insert max_retries parameter
                method_signature_pattern = r"async def find_files_by_pattern\(self,(.*?)\)"
                method_signature_match = re.search(method_signature_pattern, method_content)
                
                if method_signature_match:
                    params = method_signature_match.group(1)
                    updated_params = f"{params}, max_retries=3"
                    updated_signature = f"async def find_files_by_pattern(self,{updated_params})"
                    method_content = method_content.replace(f"async def find_files_by_pattern(self,{params})", updated_signature)
                    
                    # Add retry logic inside the method
                    try_pattern = r"try:.*?except Exception as e:"
                    try_match = re.search(try_pattern, method_content, re.DOTALL)
                    
                    if try_match:
                        try_block = try_match.group(0)
                        retry_block = """try:
            retry_count = 0
            retry_delay = 1  # Start with 1 second delay
            
            while retry_count <= max_retries:
                try:"""
                        
                        # Indent the original try block
                        indented_try = "\n".join(["                    " + line for line in try_block.split("\n")[1:]])
                        retry_block += indented_try
                        
                        # Add retry handling
                        retry_block += """
                    # If we get here, the operation succeeded
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count > max_retries:
                        # We've exceeded retries, re-raise
                        logger.error(f"Failed to list files after {max_retries} retries: {str(e)}")
                        raise
                    
                    logger.warning(f"Error listing files (retry {retry_count}/{max_retries}): {str(e)}")
                    # Exponential backoff
                    retry_delay *= 2
                    await asyncio.sleep(retry_delay)
        except Exception as e:"""
                        
                        method_content = method_content.replace(try_block, retry_block)
                        
                        # Replace the entire method in the content
                        content = content.replace(method_match.group(0), method_content)
        
        # Write the updated content
        with open(file_path, "w") as f:
            f.write(content)
        
        logger.info(f"Fixed {file_path}")
    
    async def fix_csv_processor(self):
        """Fix issues in the CSV processor cog"""
        file_path = "cogs/csv_processor.py"
        logger.info(f"Fixing {file_path}")
        
        if not os.path.exists(file_path):
            logger.error(f"File {file_path} does not exist")
            return
        
        # Read the file
        with open(file_path, "r") as f:
            content = f.read()
        
        # Fix 1: Ensure proper delimiter is passed to CSV parser
        parse_csv_pattern = r"await self\.parse_csv_data\((.*?)\)"
        
        # Find all instances of parse_csv_data calls
        for match in re.finditer(parse_csv_pattern, content):
            args = match.group(1)
            # Only add delimiter parameter if it's not already there
            if "delimiter" not in args:
                if args.endswith(")"):
                    args = args[:-1]
                if args.strip().endswith(","):
                    updated_args = f"{args} delimiter=';')"
                else:
                    updated_args = f"{args}, delimiter=';')"
                content = content.replace(match.group(0), f"await self.parse_csv_data({updated_args})")
        
        # Fix 2: Enhance error handling for CSV processing
        process_server_pattern = r"async def _process_server_csv_files.*?return files_processed, events_processed"
        process_server_match = re.search(process_server_pattern, content, re.DOTALL)
        
        if process_server_match:
            process_method = process_server_match.group(0)
            
            # Add retry logic for each file
            file_processing_pattern = r"try:.*?# Process CSV file content.*?events = .*?except Exception as e:"
            file_processing_match = re.search(file_processing_pattern, process_method, re.DOTALL)
            
            if file_processing_match:
                file_processing_block = file_processing_match.group(0)
                enhanced_processing = """try:
                    # Get file content with retries
                    retry_count = 0
                    max_retries = 3
                    retry_delay = 1
                    csv_content = None
                    
                    while retry_count <= max_retries:
                        try:
                            # Process CSV file content
                            logger.info(f"Reading CSV file {file_name}")
                            csv_content = await sftp.read_file(file_path)
                            break
                        except Exception as inner_e:
                            retry_count += 1
                            if retry_count > max_retries:
                                logger.error(f"Failed to read file {file_name} after {max_retries} retries: {str(inner_e)}")
                                raise
                            
                            logger.warning(f"Error reading file {file_name} (retry {retry_count}/{max_retries}): {str(inner_e)}")
                            retry_delay *= 2
                            await asyncio.sleep(retry_delay)
                    
                    if csv_content:
                        # Try different delimiters if first parse fails
                        events = []
                        parse_exceptions = []
                        
                        # Try semicolon first (most common)
                        try:
                            events = await parser.parse_csv_data(csv_content, delimiter=';')
                        except Exception as parse_e:
                            logger.warning(f"Failed to parse {file_name} with semicolon delimiter: {str(parse_e)}")
                            parse_exceptions.append(str(parse_e))
                        
                        # If no events parsed with semicolon, try comma
                        if not events:
                            try:
                                events = await parser.parse_csv_data(csv_content, delimiter=',')
                            except Exception as parse_e:
                                logger.warning(f"Failed to parse {file_name} with comma delimiter: {str(parse_e)}")
                                parse_exceptions.append(str(parse_e))
                        
                        # If still no events, try tab
                        if not events:
                            try:
                                events = await parser.parse_csv_data(csv_content, delimiter='\\t')
                            except Exception as parse_e:
                                logger.warning(f"Failed to parse {file_name} with tab delimiter: {str(parse_e)}")
                                parse_exceptions.append(str(parse_e))
                        
                        # If all parsing attempts failed, log comprehensive error
                        if not events and parse_exceptions:
                            error_details = ", ".join(parse_exceptions)
                            logger.error(f"All parsing attempts failed for {file_name}: {error_details}")
                            
                            # Additionally, log a sample of the file content for debugging
                            content_sample = csv_content[:200] + "..." if len(csv_content) > 200 else csv_content
                            logger.error(f"Content sample: {content_sample}")
                    else:
                        events = []
                except Exception as e:"""
                
                process_method = process_method.replace(file_processing_block, enhanced_processing)
                content = content.replace(process_server_match.group(0), process_method)
        
        # Write the updated content
        with open(file_path, "w") as f:
            f.write(content)
        
        logger.info(f"Fixed {file_path}")
    
    async def test_fixes(self):
        """Test the fixes with sample CSV files"""
        logger.info("Testing fixes with sample CSV files")
        
        try:
            # Import the CSV parser
            import importlib
            import sys
            
            sys.path.append('.')
            
            # Force reload of the modules
            if 'utils.csv_parser' in sys.modules:
                del sys.modules['utils.csv_parser']
            
            from utils.csv_parser import CSVParser
            
            # Test with a sample CSV from attached_assets
            csv_file = "attached_assets/2025.05.09-11.58.37.csv"
            
            if not os.path.exists(csv_file):
                logger.error(f"Sample CSV file {csv_file} not found")
                return False
            
            # Read the file content
            with open(csv_file, "r") as f:
                csv_content = f.read()
            
            # Create parser instance
            parser = CSVParser()
            
            # Test parsing with different delimiters
            events_semicolon = parser.parse_csv_data(csv_content, delimiter=';')
            logger.info(f"Parsed {len(events_semicolon)} events with semicolon delimiter")
            
            # Check if events were parsed correctly
            if not events_semicolon:
                logger.error("No events parsed with semicolon delimiter")
                return False
            
            # Verify the timestamp is parsed correctly
            if events_semicolon and isinstance(events_semicolon[0].get('timestamp'), datetime):
                logger.info("Timestamp parsed correctly")
            else:
                logger.error("Timestamp not parsed correctly")
                return False
            
            logger.info("CSV parsing tests passed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error during testing: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def post_success_message(self):
        """Post a success message to Discord if possible"""
        try:
            import discord
            import os
            
            # Get Discord token if available
            token = os.environ.get('DISCORD_TOKEN')
            if not token:
                logger.warning("No Discord token found, skipping success message")
                return
            
            # Create Discord client
            intents = discord.Intents.default()
            client = discord.Client(intents=intents)
            
            @client.event
            async def on_ready():
                try:
                    logger.info(f"Connected to Discord as {client.user}")
                    
                    # Try to get the bot-2 channel or similar
                    channel_id = 1360632422957449237  # Known channel ID
                    channel = client.get_channel(channel_id)
                    
                    if not channel:
                        try:
                            channel = await client.fetch_channel(channel_id)
                        except Exception as e:
                            logger.error(f"Error fetching channel: {e}")
                            await client.close()
                            return
                    
                    if channel:
                        # Create embed
                        embed = discord.Embed(
                            title="CSV Parsing Fix",
                            description="✅ Successfully fixed CSV parsing issues:\n\n" +
                                      "• Improved delimiter detection with priority for semicolons\n" +
                                      "• Enhanced timestamp parsing with additional formats\n" +
                                      "• Updated file pattern recognition to be more inclusive\n" +
                                      "• Added better error handling and diagnostics\n\n" +
                                      "The bot should now properly find and parse CSV files.",
                            color=discord.Color.green()
                        )
                        
                        embed.timestamp = datetime.now()
                        
                        await channel.send(embed=embed)
                    
                    await client.close()
                
                except Exception as e:
                    logger.error(f"Error posting success message: {e}")
                    await client.close()
            
            # Run client to post message
            await client.start(token)
            
        except Exception as e:
            logger.error(f"Failed to post success message: {e}")

async def main():
    """Main entry point"""
    fixer = ComprehensiveCSVFix()
    await fixer.run()

# Run the fix if script is executed directly
if __name__ == "__main__":
    asyncio.run(main())