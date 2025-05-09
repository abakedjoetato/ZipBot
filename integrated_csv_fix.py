"""
Integrated CSV Processing Fix

This script implements a complete fix for CSV processing issues in the Tower of Temptation bot:
1. Improves delimiter detection with priority for semicolons
2. Enhances timestamp parsing with additional formats
3. Makes row validation more permissive
4. Improves error handling for malformed timestamps

The fixes are applied directly to the utils/csv_parser.py file with automatic testing.
"""

import os
import sys
import re
import logging
import shutil
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

def backup_file(file_path):
    """Create a backup of the specified file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{file_path}.bak_{timestamp}"
    shutil.copy2(file_path, backup_path)
    logger.info(f"Created backup at {backup_path}")
    return backup_path

def fix_delimiter_detection(content):
    """Fix delimiter detection to prioritize semicolons"""
    logger.info("Enhancing delimiter detection...")
    
    delimiter_pattern = re.compile(
        r'(\s+# Count occurrences of potential delimiters.*?'
        r'for d in delimiters:.*?'
        r'\s+delimiters\[d\] = file_content\.count\(d\))',
        re.DOTALL
    )
    
    delimiter_replacement = r"""\1
        
        # Add extra weight to semicolons to handle mixed format files better
        # Game logs commonly use semicolons and we want to prioritize them
        if delimiters.get(';', 0) > 0:
            delimiters[';'] *= 1.5  # Give semicolons a 50% boost in detection
            logger.debug(f"Boosting semicolon count from {delimiters[';']/1.5} to {delimiters[';']}")"""
    
    return delimiter_pattern.sub(delimiter_replacement, content)

def enhance_timestamp_formats(content):
    """Add more timestamp formats for better compatibility"""
    logger.info("Enhancing timestamp format detection...")
    
    # Simple approach: replace the entire timestamp formats list
    formats_pattern = re.compile(
        r'alternative_formats = \[.*?\]',
        re.DOTALL
    )
    
    formats_replacement = '''alternative_formats = [
                            "%Y.%m.%d-%H.%M.%S",      # 2025.03.27-10.42.18 (primary format)
                            "%Y.%m.%d-%H:%M:%S",      # 2025.05.09-11:58:37 (variant with colons)
                            "%Y.%m.%d %H.%M.%S",      # 2025.05.09 11.58.37 (space instead of dash)
                            "%Y.%m.%d %H:%M:%S",      # 2025.05.09 11:58:37
                            "%Y-%m-%d-%H.%M.%S",      # 2025-05-09-11.58.37
                            "%Y-%m-%d %H:%M:%S",      # 2025-05-09 11:58:37
                            "%Y/%m/%d %H:%M:%S",      # 2025/05/09 11:58:37
                            "%d.%m.%Y-%H.%M.%S",      # 09.05.2025-11.58.37
                            "%d.%m.%Y %H:%M:%S",      # 09.05.2025 11:58:37
                            "%d-%m-%Y %H:%M:%S"       # 09-05-2025 11:58:37
                        ]'''
    
    return formats_pattern.sub(formats_replacement, content)

def improve_timestamp_error_handling(content):
    """Improve error handling for timestamp parsing failures"""
    logger.info("Improving timestamp error handling...")
    
    # Replace the entire timestamp error handling block
    error_pattern = re.compile(
        r'if not parsed:.*?# Keep original string if all parsing attempts fail',
        re.DOTALL
    )
    
    error_replacement = '''if not parsed:
                            logger.warning(f"Failed to parse timestamp: '{timestamp_str}' - keeping as string")
                            # Keep the original string value instead of failing completely
                            event[self.datetime_column] = timestamp_str
                            # Add a flag indicating this event has an unparsed timestamp
                            event['_timestamp_parse_failed'] = True
                            # Continue processing - don't skip the whole row'''
    
    return error_pattern.sub(error_replacement, content)

def improve_row_validation(content):
    """Make row validation more permissive for partial rows"""
    logger.info("Improving row validation logic...")
    
    validation_pattern = re.compile(
        r'(\s+# Skip empty rows\s+)(if row is None or len\(row\) < 6:.*?continue)',
        re.DOTALL
    )
    
    validation_replacement = r"""\1if row is None:
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
    
    return validation_pattern.sub(validation_replacement, content)

def test_csv_parsing():
    """Test CSV parsing with the sample files to verify the fix works"""
    logger.info("Testing CSV parsing with sample files...")
    
    # Import the updated module
    try:
        # Force reload
        if 'utils.csv_parser' in sys.modules:
            del sys.modules['utils.csv_parser']
        
        sys.path.append('.')
        from utils.csv_parser import CSVParser
        
        # Test with the sample file
        test_file = "attached_assets/2025.05.09-11.58.37.csv"
        
        if not os.path.exists(test_file):
            logger.error(f"Test file not found: {test_file}")
            return False
        
        with open(test_file, "r") as f:
            content = f.read()
        
        parser = CSVParser()
        
        # Test parsing with semicolon
        events = parser.parse_csv_data(content, delimiter=";")
        if not events:
            logger.error("Failed to parse sample file with semicolon delimiter")
            return False
        
        logger.info(f"Successfully parsed {len(events)} events from sample file")
        logger.info(f"First event: {events[0]}")
        
        # Check timestamp parsing
        if not isinstance(events[0].get('timestamp'), datetime):
            logger.error("Timestamp parsing failed")
            return False
        
        logger.info("Timestamp parsing successful")
        return True
    
    except Exception as e:
        logger.error(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False

def apply_all_fixes():
    """Apply all fixes to the CSV parser file"""
    file_path = "utils/csv_parser.py"
    
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return False
    
    try:
        # Create backup
        backup_path = backup_file(file_path)
        
        # Read the current content
        with open(file_path, "r") as f:
            content = f.read()
        
        # Apply all fixes
        content = fix_delimiter_detection(content)
        content = enhance_timestamp_formats(content)
        content = improve_timestamp_error_handling(content)
        content = improve_row_validation(content)
        
        # Write the fixed content
        with open(file_path, "w") as f:
            f.write(content)
        
        logger.info(f"All fixes applied to {file_path}")
        
        # Test the fixes
        success = test_csv_parsing()
        
        if success:
            logger.info("✅ CSV parsing fix verified successfully")
            return True
        else:
            logger.error("⚠️ CSV parsing test failed, restoring backup")
            shutil.copy2(backup_path, file_path)
            return False
    
    except Exception as e:
        logger.error(f"Error applying fixes: {e}")
        import traceback
        traceback.print_exc()
        
        # Try to restore from backup if we made one
        if 'backup_path' in locals():
            logger.info(f"Restoring from backup: {backup_path}")
            shutil.copy2(backup_path, file_path)
        
        return False

if __name__ == "__main__":
    logger.info("Starting integrated CSV fix")
    if apply_all_fixes():
        logger.info("CSV processing fix completed successfully")
        sys.exit(0)
    else:
        logger.error("CSV processing fix failed")
        sys.exit(1)