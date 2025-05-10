#!/usr/bin/env python3
"""
Comprehensive CSV Processing Fix Implementation

This script applies all necessary fixes to the CSV processing system
to ensure proper parsing of all CSV files with various delimiters and formats.
"""

import os
import sys
import shutil
import logging
import re
from datetime import datetime

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

def backup_file(file_path):
    """Create a backup of the file before modifying it"""
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    backup_path = f"{file_path}.{timestamp}.backup"
    shutil.copy2(file_path, backup_path)
    logger.info(f"Created backup at {backup_path}")
    return backup_path

def fix_csv_parser():
    """Apply fixes to the CSV parser module"""
    parser_path = "utils/csv_parser.py"
    if not os.path.exists(parser_path):
        logger.error(f"CSV parser file not found at {parser_path}")
        return False
    
    # Backup the original file
    backup_file(parser_path)
    
    # Read the file
    with open(parser_path, 'r') as f:
        content = f.read()
    
    # Fix 1: Improve delimiter detection for semicolons
    delimiter_pattern = r"delimiter_weights\s*=\s*{[^}]*';'\s*:\s*(\d+(\.\d+)?)"
    match = re.search(delimiter_pattern, content)
    if match:
        original_weight = float(match.group(1))
        new_weight = max(original_weight * 2, 100)  # Double the weight or set to 100
        logger.info(f"Boosting semicolon delimiter weight from {original_weight} to {new_weight}")
        
        content = re.sub(
            r"(delimiter_weights\s*=\s*{[^}]*';'\s*:\s*)(\d+(\.\d+)?)",
            f"\\1{new_weight}",
            content
        )
    else:
        logger.warning("Could not find semicolon delimiter weight to update")
    
    # Fix 2: Add pattern-based detection for semicolons
    detect_pattern = r"def _detect_delimiter\(self, content:.*?\):"
    match = re.search(detect_pattern, content, re.DOTALL)
    if match:
        start_pos = match.end()
        end_pos = content.find("return delimiter", start_pos)
        if end_pos > 0:
            pattern_code = """
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
            
            insertion_point = end_pos - 1
            content = content[:insertion_point] + pattern_code + content[insertion_point:]
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
    
    logger.info(f"Successfully applied CSV parser fixes to {parser_path}")
    return True

def fix_sftp_module():
    """Apply fixes to the SFTP module for better CSV file pattern matching"""
    sftp_path = "utils/sftp.py"
    if not os.path.exists(sftp_path):
        logger.error(f"SFTP module file not found at {sftp_path}")
        return False
    
    # Backup the original file
    backup_file(sftp_path)
    
    # Read the file
    with open(sftp_path, 'r') as f:
        content = f.read()
    
    # Fix: Enhance CSV file pattern matching
    csv_pattern_match = re.search(r"csv_pattern\s*=\s*r\"([^\"]+)\"", content)
    if csv_pattern_match:
        original_pattern = csv_pattern_match.group(1)
        logger.info(f"Original CSV pattern: {original_pattern}")
        
        # Find a suitable place to add enhanced pattern detection
        class_init_match = re.search(r"class SFTPClient.*?def __init__\(", content, re.DOTALL)
        if class_init_match:
            enhanced_patterns = """
    # Enhanced patterns for CSV filenames with various date/time formats
    csv_patterns = [
        # Standard format: 2025.03.27-00.00.00.csv
        r"\\d{4}\\.\\d{2}\\.\\d{2}-\\d{2}\\.\\d{2}\\.\\d{2}\\.csv$",
        
        # Alternate format with colons: 2025.05.09-11:58:37.csv
        r"\\d{4}\\.\\d{2}\\.\\d{2}-\\d{2}:\\d{2}:\\d{2}\\.csv$",
        
        # ISO format: 2025-05-09-11.58.37.csv or 2025-05-09T11:58:37.csv
        r"\\d{4}-\\d{2}-\\d{2}[T-]\\d{2}[:\\.]\\d{2}[:\\.]\\d{2}\\.csv$",
        
        # Format with underscores: 2025_05_09_11_58_37.csv
        r"\\d{4}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_\\d{2}\\.csv$",
        
        # Format with spaces: 2025.05.09 11.58.37.csv
        r"\\d{4}\\.\\d{2}\\.\\d{2} \\d{2}\\.\\d{2}\\.\\d{2}\\.csv$",
        
        # Compact format: 20250509-115837.csv
        r"\\d{8}-\\d{6}\\.csv$",
        
        # European format: 09.05.2025-11.58.37.csv
        r"\\d{2}\\.\\d{2}\\.\\d{4}-\\d{2}\\.\\d{2}\\.\\d{2}\\.csv$",
        
        # Any CSV that might contain timestamp patterns
        r".*\\d{2,4}[.-]\\d{2}[.-]\\d{2,4}.*\\.csv$"
    ]
    
    def is_csv_file(self, filename):
        \"\"\"Check if filename matches any CSV pattern
        
        Args:
            filename: The filename to check
            
        Returns:
            bool: True if the filename matches a CSV pattern, False otherwise
        \"\"\"
        return filename.endswith('.csv') and any(re.match(pattern, filename) for pattern in self.csv_patterns)
    """
            
            # Insert at appropriate position (after class init)
            insert_pos = class_init_match.end()
            content = content[:insert_pos] + enhanced_patterns + content[insert_pos:]
            
            # Update existing CSV pattern checks to use the new method
            content = re.sub(
                r"if re\.match\(self\.csv_pattern, filename\)",
                "if self.is_csv_file(filename)",
                content
            )
            
            # Keep the original pattern but mark it as deprecated
            content = re.sub(
                r"(csv_pattern\s*=\s*r\"[^\"]+\")",
                r"# DEPRECATED: \1 - Using enhanced patterns instead",
                content
            )
            
            logger.info("Added enhanced CSV pattern matching to SFTP module")
        else:
            logger.warning("Could not find suitable location to add enhanced CSV patterns")
    else:
        logger.warning("Could not find original CSV pattern in SFTP module")
    
    # Write updated content to file
    with open(sftp_path, 'w') as f:
        f.write(content)
    
    logger.info(f"Successfully applied SFTP module fixes to {sftp_path}")
    return True

def main():
    """Main function to apply all fixes"""
    logger.info("Starting comprehensive CSV fixes...")
    
    # Apply CSV parser fixes
    if fix_csv_parser():
        logger.info("Successfully applied CSV parser fixes")
    else:
        logger.error("Failed to apply CSV parser fixes")
    
    # Apply SFTP module fixes
    if fix_sftp_module():
        logger.info("Successfully applied SFTP module fixes")
    else:
        logger.error("Failed to apply SFTP module fixes")
    
    logger.info("CSV fixes complete - Run tests to verify")

if __name__ == "__main__":
    main()