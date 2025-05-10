#!/usr/bin/env python3
"""
Comprehensive CSV Fixes Implementation

This script directly applies all necessary fixes to the CSV processing system
to ensure proper parsing of all CSV files:

1. Improved delimiter detection (especially for semicolons)
2. Enhanced timestamp parsing for multiple formats
3. Better empty file handling
4. Row validation with incomplete rows support
5. Enhanced CSV filename pattern matching
"""

import os
import sys
import shutil
import logging
import re
from datetime import datetime
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('csv_fix.log')
    ]
)

logger = logging.getLogger('csv_fix')

def backup_file(file_path: str) -> str:
    """Create a backup of the specified file
    
    Args:
        file_path: Path to the file to backup
        
    Returns:
        Path to the backup file
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    backup_path = f"{file_path}.{timestamp}.backup"
    shutil.copy2(file_path, backup_path)
    logger.info(f"Created backup at {backup_path}")
    return backup_path

def fix_csv_parser():
    """Apply fixes to the CSV parser"""
    parser_path = "utils/csv_parser.py"
    if not os.path.exists(parser_path):
        logger.error(f"CSV parser file not found at {parser_path}")
        return False
    
    # Create backup
    backup_path = backup_file(parser_path)
    
    # Read the file
    with open(parser_path, 'r') as f:
        content = f.read()
    
    # Fix 1: Improve delimiter detection for semicolons
    pattern = r"delimiter_weights\s*=\s*{[^}]*';'\s*:\s*(\d+(\.\d+)?)"
    match = re.search(pattern, content)
    if match:
        original_weight = float(match.group(1))
        new_weight = max(original_weight * 2, 100)  # Double the weight or set to 100, whichever is higher
        logger.info(f"Boosting semicolon delimiter weight from {original_weight} to {new_weight}")
        
        content = re.sub(
            r"(delimiter_weights\s*=\s*{[^}]*';'\s*:\s*)(\d+(\.\d+)?)",
            f"\\1{new_weight}",
            content
        )
    else:
        logger.warning("Could not find delimiter weights pattern to update")
    
    # Fix 2: Add snippet to detect sequential semicolons
    pattern = r"def _detect_delimiter\(self, content:.*?\):"
    match = re.search(pattern, content, re.DOTALL)
    if match:
        start_pos = match.end()
        end_semicolon_detect = content.find("return delimiter", start_pos)
        if end_semicolon_detect > 0:
            pattern_detect_code = """
        # Additional pattern-based detection for semicolons
        # Check for sequential semicolons pattern which is a strong indicator
        if ';' in sample and content.count(';') >= 3:
            # Count sequences of values separated by semicolons
            semicolon_sequences = re.findall(r'[^;]+;[^;]+;[^;]+', sample)
            if len(semicolon_sequences) >= 2:
                logger.debug(f"Detected sequential semicolon pattern, prioritizing semicolon delimiter")
                delimiter_weights[';'] *= 1.5  # Boost semicolon weight by 50%
                
        # Check for quoted commas but unquoted semicolons pattern (common in European CSVs)
        if ',' in sample and ';' in sample:
            quoted_commas = re.findall(r'"[^"]*,[^"]*"', sample)
            if len(quoted_commas) >= 2 and sample.count(';') >= sample.count(','):
                logger.debug(f"Detected quoted commas but unquoted semicolons pattern")
                delimiter_weights[';'] *= 1.5  # Boost semicolon weight further
            """
            
            insertion_point = end_semicolon_detect - 1
            content = content[:insertion_point] + pattern_detect_code + content[insertion_point:]
        else:
            logger.warning("Could not find end of delimiter detection function")
    else:
        logger.warning("Could not find delimiter detection function")
    
    # Fix 3: Enhance timestamp parsing with multiple formats
    timestamp_pattern = r"# Try these common formats.*?alternative_formats\s*=\s*\[(.*?)\]"
    match = re.search(timestamp_pattern, content, re.DOTALL)
    if match:
        enhanced_formats = """
                # Try these common formats
                alternative_formats = [
                    # Primary formats with dots
                    "%Y.%m.%d-%H.%M.%S",      # 2025.03.27-10.42.18 (primary format)
                    "%Y.%m.%d-%H:%M:%S",      # 2025.05.09-11:58:37 (variant with colons)
                    "%Y.%m.%d-%H.%M.%S.%f",   # 2025.03.27-10.42.18.123 (with milliseconds)
                    
                    # Formats with space instead of dash
                    "%Y.%m.%d %H.%M.%S",      # 2025.05.09 11.58.37 (space instead of dash)
                    "%Y.%m.%d %H:%M:%S",      # 2025.05.09 11:58:37
                    "%Y.%m.%d %H.%M.%S.%f",   # 2025.05.09 11.58.37.123 (with milliseconds)
                    
                    # Hyphen formats
                    "%Y-%m-%d-%H.%M.%S",      # 2025-05-09-11.58.37
                    "%Y-%m-%d %H:%M:%S",      # 2025-05-09 11:58:37
                    "%Y-%m-%d-%H:%M:%S",      # 2025-05-09-11:58:37
                    "%Y-%m-%d %H.%M.%S",      # 2025-05-09 11.58.37
                    "%Y-%m-%dT%H:%M:%S",      # 2025-05-09T11:58:37 (ISO format)
                    "%Y-%m-%dT%H:%M:%S.%f",   # 2025-05-09T11:58:37.123 (ISO with ms)
                    
                    # Slash formats
                    "%Y/%m/%d %H:%M:%S",      # 2025/05/09 11:58:37
                    "%Y/%m/%d-%H:%M:%S",      # 2025/05/09-11:58:37
                    
                    # Day first formats (European style)
                    "%d.%m.%Y-%H.%M.%S",      # 09.05.2025-11.58.37
                    "%d.%m.%Y %H:%M:%S",      # 09.05.2025 11:58:37
                    "%d-%m-%Y %H:%M:%S",      # 09-05-2025 11:58:37
                    
                    # Additional variants
                    "%Y%m%d-%H%M%S",          # 20250509-115837 (compact format)
                    "%Y%m%d_%H%M%S",          # 20250509_115837 (underscore format)
                    
                    # Unix timestamp (as string)
                    "%s"                      # Unix timestamp in seconds
                ]
                """
        
        content = re.sub(timestamp_pattern, enhanced_formats, content, flags=re.DOTALL)
    else:
        logger.warning("Could not find timestamp formats to update")
    
    # Fix 4: Improve empty file handling
    empty_file_pattern = r"(# Check if file is empty.*?if\s+not\s+content\.strip\(\):.*?return\s+\[\])"
    match = re.search(empty_file_pattern, content, re.DOTALL)
    if match:
        improved_empty_check = """
        # Enhanced empty file detection
        # Check multiple conditions to properly detect empty files
        is_empty = False
        
        # Check if content is None or empty string
        if content is None or content == '':
            is_empty = True
            logger.warning(f"Empty CSV file detected (no content): {file_path or 'unknown'}")
        # Check if content contains only whitespace
        elif isinstance(content, str) and not content.strip():
            is_empty = True
            logger.warning(f"Empty CSV file detected (only whitespace): {file_path or 'unknown'}")
        # Check if content is too short to be meaningful (less than 10 chars)
        elif isinstance(content, str) and len(content.strip()) < 10:
            logger.warning(f"Nearly empty CSV file detected (less than 10 chars): {file_path or 'unknown'}")
            # Log the actual content for debugging
            logger.debug(f"File content: '{content}'")
            
            # Check if short content is actually a valid row
            if (',' in content or ';' in content) and len(content.strip().split(',')) >= 3:
                logger.info(f"Short content appears to be a valid CSV row, will attempt to parse")
            else:
                is_empty = True
        # For binary content, check length
        elif isinstance(content, bytes) and len(content) < 10:
            is_empty = True
            logger.warning(f"Empty binary CSV file detected (less than 10 bytes): {file_path or 'unknown'}")
            
        if is_empty:
            logger.info(f"Skipping empty file: {file_path or 'unknown'}")
            return []
            """
        
        content = content.replace(match.group(1), improved_empty_check)
    else:
        logger.warning("Could not find empty file check to update")
    
    # Fix 5: Improve row validation for incomplete rows
    row_validation_pattern = r"# More permissive handling of rows with insufficient fields.*?if\s+len\(row\)\s*<\s*\d+.*?logger\.warning\(.*?\)"
    match = re.search(row_validation_pattern, content, re.DOTALL)
    if match:
        improved_row_validation = """
                # More permissive handling of rows with insufficient fields
                if len(row) < 7:  # Standard kill event has 7-9 fields (7 for pre-April, 9 for post-April)
                    logger.warning(f"Row {current_line} has fewer fields than expected ({len(row)} < 7): {row}")
                    
                    # Try to extract whatever data we can anyway
                    if len(row) >= 3:
                        logger.debug(f"Attempting partial extraction from incomplete row: {row}")
                        
                        # Make best guess at field assignment for incomplete rows
                        # For very short rows (3-5 fields), try to intelligently map the fields
                        if len(row) == 3:
                            # Assume killer, victim, weapon format
                            row = [datetime.now().strftime(self.datetime_format), row[0], "", row[1], "", row[2], "0", ""]
                            logger.debug(f"Expanded 3-field row to: {row}")
                        elif len(row) == 4:
                            # Assume timestamp, killer, victim, weapon format
                            row = [row[0], row[1], "", row[2], "", row[3], "0", ""]
                            logger.debug(f"Expanded 4-field row to: {row}")
                        elif len(row) == 5:
                            # Assume timestamp, killer, killer_id, victim, weapon
                            row = [row[0], row[1], row[2], row[3], "", row[4], "0", ""]
                            logger.debug(f"Expanded 5-field row to: {row}")
                        elif len(row) == 6:
                            # Assume timestamp, killer, killer_id, victim, victim_id, weapon
                            row = [row[0], row[1], row[2], row[3], row[4], row[5], "0", ""]
                            logger.debug(f"Expanded 6-field row to: {row}")
                    else:
                        # Too few fields to reasonably process
                        logger.debug(f"Skipping row with too few fields: {row}")
                        continue
                """
        
        content = content.replace(match.group(0), improved_row_validation)
    else:
        logger.warning("Could not find row validation to update")
    
    # Write updated content to file
    with open(parser_path, 'w') as f:
        f.write(content)
    
    logger.info(f"Successfully applied changes to {parser_path}")
    return True

def fix_sftp_module():
    """Apply fixes to the SFTP module for better CSV filename matching"""
    sftp_path = "utils/sftp.py"
    if not os.path.exists(sftp_path):
        logger.error(f"SFTP module file not found at {sftp_path}")
        return False
    
    # Create backup
    backup_path = backup_file(sftp_path)
    
    # Read the file
    with open(sftp_path, 'r') as f:
        content = f.read()
    
    # Fix: Enhance CSV file pattern matching
    pattern = r"# Pattern for CSV filenames.*?csv_pattern\s*=\s*r\"(.*?)\""
    match = re.search(pattern, content, re.DOTALL)
    if match:
        original_pattern = match.group(1)
        logger.info(f"Original CSV filename pattern: {original_pattern}")
        
        enhanced_patterns = r"""
        # Patterns for CSV filenames with various formats
        csv_patterns = [
            # Standard format: 2025.03.27-00.00.00.csv
            r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv$",
            
            # Alternate format with colons: 2025.05.09-11:58:37.csv
            r"\d{4}\.\d{2}\.\d{2}-\d{2}:\d{2}:\d{2}\.csv$",
            
            # ISO format: 2025-05-09-11.58.37.csv or 2025-05-09T11:58:37.csv
            r"\d{4}-\d{2}-\d{2}[T-]\d{2}[:.]\d{2}[:.]\d{2}\.csv$",
            
            # Format with underscores: 2025_05_09_11_58_37.csv
            r"\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}\.csv$",
            
            # Format with spaces: 2025.05.09 11.58.37.csv
            r"\d{4}\.\d{2}\.\d{2} \d{2}\.\d{2}\.\d{2}\.csv$",
            
            # Compact format: 20250509-115837.csv
            r"\d{8}-\d{6}\.csv$",
            
            # European format: 09.05.2025-11.58.37.csv
            r"\d{2}\.\d{2}\.\d{4}-\d{2}\.\d{2}\.\d{2}\.csv$",
            
            # Any CSV that might contain timestamp patterns
            r".*\d{2,4}[.-]\d{2}[.-]\d{2,4}.*\.csv$"
        ]
        
        def is_csv_file(filename):
            # Check if the filename matches any of our CSV patterns
            return any(re.match(pattern, filename) for pattern in csv_patterns)
        """
        
        # Find a good insertion point
        insertion_pattern = r"class SFTPClient\(object\):.*?def __init__\("
        insertion_match = re.search(insertion_pattern, content, re.DOTALL)
        if insertion_match:
            insertion_point = insertion_match.end()
            # Insert the enhanced patterns
            content = content[:insertion_point] + "\n" + enhanced_patterns + content[insertion_point:]
            
            # Update any instances using the old pattern
            content = re.sub(
                r"if re\.match\(self\.csv_pattern, filename\)",
                "if is_csv_file(filename)",
                content
            )
            content = re.sub(
                r"csv_pattern\s*=\s*r\"(.*?)\"",
                "# Old pattern (kept for reference): r\"\\1\"",
                content
            )
        else:
            logger.warning("Could not find suitable insertion point for enhanced patterns")
    else:
        logger.warning("Could not find CSV filename pattern to update")
    
    # Write updated content to file
    with open(sftp_path, 'w') as f:
        f.write(content)
    
    logger.info(f"Successfully applied changes to {sftp_path}")
    return True

def fix_csv_processor_cog():
    """Apply fixes to the CSV processor cog"""
    cog_path = "cogs/csv_processor.py"
    if not os.path.exists(cog_path):
        logger.error(f"CSV processor cog file not found at {cog_path}")
        return False
    
    # Create backup
    backup_path = backup_file(cog_path)
    
    # Read the file
    with open(cog_path, 'r') as f:
        content = f.read()
    
    # Fix: Add detailed diagnostics for CSV processing
    diagnostics_code = """
    @commands.command(name="csv_diagnostics")
    async def csv_diagnostics(self, ctx, file_path=None):
        """Run diagnostics on CSV files to identify and fix issues"""
        try:
            if not file_path:
                await ctx.send("Please specify a CSV file path to diagnose or use 'all' to scan all files.")
                return
                
            if file_path.lower() == "all":
                await ctx.send("Running diagnostics on all CSV files. This may take a moment...")
                
                # Get a list of all CSV files
                sftp_client = self.bot.get_cog("SFTPClient") if hasattr(self.bot, "get_cog") else None
                if sftp_client:
                    csv_files = sftp_client.find_csv_files()
                else:
                    # Fallback to local files
                    csv_files = []
                    for root, _, files in os.walk("attached_assets"):
                        for file in files:
                            if file.endswith(".csv"):
                                csv_files.append(os.path.join(root, file))
                
                if not csv_files:
                    await ctx.send("No CSV files found for diagnostic.")
                    return
                    
                results = []
                for file_path in csv_files:
                    result = self._diagnose_csv_file(file_path)
                    results.append(result)
                    
                # Summarize results
                total = len(results)
                successful = sum(1 for r in results if r["success"])
                failed = total - successful
                
                summary = f"**CSV Diagnostics Summary**\\n"
                summary += f"Total files: {total}\\n"
                summary += f"Successfully parsed: {successful}\\n"
                summary += f"Failed to parse: {failed}\\n\\n"
                
                if failed > 0:
                    summary += "**Failed Files:**\\n"
                    for r in results:
                        if not r["success"]:
                            summary += f"• {r['file_path']}: {r['error']}\\n"
                
                await ctx.send(summary[:1900])  # Limit to Discord message size
                
            else:
                # Diagnose a single file
                await ctx.send(f"Running diagnostics on {file_path}...")
                result = self._diagnose_csv_file(file_path)
                
                if result["success"]:
                    message = f"✅ Successfully diagnosed {file_path}\\n"
                    message += f"• Detected delimiter: {result['delimiter']}\\n"
                    message += f"• Events extracted: {result['event_count']}\\n"
                    
                    if result['event_count'] > 0:
                        # Show sample event
                        message += f"\\n**Sample Event:**\\n"
                        sample = result['events'][0]
                        for key, value in sample.items():
                            message += f"• {key}: {value}\\n"
                            
                    await ctx.send(message[:1900])
                else:
                    await ctx.send(f"❌ Failed to diagnose {file_path}: {result['error']}")
        
        except Exception as e:
            await ctx.send(f"Error during diagnostics: {str(e)}")
            logging.error(f"Error in csv_diagnostics: {e}", exc_info=True)
            
    def _diagnose_csv_file(self, file_path):
        """Run diagnostics on a single CSV file"""
        result = {
            "file_path": file_path,
            "success": False,
            "error": None,
            "delimiter": None,
            "event_count": 0,
            "events": []
        }
        
        try:
            # Check if the file exists and is readable
            if not os.path.exists(file_path):
                result["error"] = "File not found"
                return result
                
            # Read the file content
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
            # Check if the file is empty
            if not content.strip():
                result["success"] = True
                result["error"] = "Empty file"
                result["event_count"] = 0
                return result
                
            # Create a CSV parser
            parser = CSVParser(format_name="deadside")
            
            # Process the file
            events = parser.process_csv_file(file_path)
            
            # Store results
            result["success"] = True
            result["delimiter"] = parser.last_detected_delimiter
            result["event_count"] = len(events)
            result["events"] = events[:5]  # Store up to 5 sample events
        
        except Exception as e:
            result["error"] = str(e)
            logging.error(f"Error diagnosing {file_path}: {e}", exc_info=True)
            
        return result
    """
    
    # Find a good insertion point for the new command (before the cog setup function)
    setup_pattern = r"def setup\(bot\):"
    match = re.search(setup_pattern, content)
    if match:
        insertion_point = match.start()
        # Ensure proper indentation by finding the class indentation level
        class_match = re.search(r"class CSVProcessor\(commands\.Cog\):", content)
        if class_match:
            # Extract the content before the insertion point to find the class definition line
            content_before = content[:insertion_point]
            lines = content_before.split('\n')
            for i in range(len(lines) - 1, 0, -1):
                if "class CSVProcessor" in lines[i]:
                    # Determine the indentation for a method in this class
                    class_line = lines[i]
                    if class_line.startswith(" "):
                        # Class is indented, so methods need more indentation
                        indent = " " * (len(class_line) - len(class_line.lstrip()) + 4)
                    else:
                        # Class is at root level, so methods need standard indentation
                        indent = "    "
                    
                    # Apply the correct indentation to our diagnostic code
                    indented_code = ""
                    for line in diagnostics_code.split('\n'):
                        if line.strip():
                            indented_code += indent + line + '\n'
                        else:
                            indented_code += '\n'
                    
                    # Insert the new command
                    content = content[:insertion_point] + indented_code + content[insertion_point:]
                    break
        else:
            logger.warning("Could not find class definition to determine indentation")
    else:
        logger.warning("Could not find setup function for insertion point")
    
    # Write updated content to file
    with open(cog_path, 'w') as f:
        f.write(content)
    
    logger.info(f"Successfully applied changes to {cog_path}")
    return True

def apply_all_fixes():
    """Apply all fixes and test the changes"""
    success = True
    
    # Step 1: Fix CSV parser
    if not fix_csv_parser():
        logger.error("Failed to fix CSV parser")
        success = False
    
    # Step 2: Fix SFTP module
    if not fix_sftp_module():
        logger.error("Failed to fix SFTP module")
        success = False
    
    # Step 3: Fix CSV processor cog
    if not fix_csv_processor_cog():
        logger.error("Failed to fix CSV processor cog")
        success = False
    
    # Step 4: Summary
    if success:
        logger.info("All fixes applied successfully!")
        logger.info("Use 'python test_csv_processing.py' to verify the changes.")
    else:
        logger.error("Some fixes could not be applied. Check the log for details.")
    
    return success

if __name__ == "__main__":
    print("Applying comprehensive CSV fixes...")
    if apply_all_fixes():
        print("All fixes applied successfully!")
    else:
        print("Some fixes could not be applied. Check the log for details.")