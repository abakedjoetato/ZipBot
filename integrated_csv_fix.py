#!/usr/bin/env python3
"""
Integrated CSV Fix for the Tower of Temptation Discord Bot

This script contains a comprehensive solution for the CSV parsing issues
with the Discord bot, addressing the following problems:
1. Delimiter detection failure (especially for semicolon-delimited files)
2. Timestamp parsing errors with various date formats
3. Empty CSV file handling errors
4. Issues with malformed or incomplete rows

Usage:
    python integrated_csv_fix.py [--apply] [--test]
    
    --apply: Apply the fixes to the main codebase
    --test: Run tests on the sample CSV files
"""
import os
import sys
import glob
import shutil
import logging
import argparse
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('csv_fix.log')
    ]
)
logger = logging.getLogger('integrated_csv_fix')

def backup_file(file_path):
    """Create a backup of the file before modifying it"""
    if not os.path.exists(file_path):
        logger.warning(f"Cannot backup non-existent file: {file_path}")
        return None
    
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_path = f"{file_path}.{timestamp}.backup"
    try:
        shutil.copy2(file_path, backup_path)
        logger.info(f"Created backup at {backup_path}")
        return backup_path
    except Exception as e:
        logger.error(f"Failed to create backup of {file_path}: {str(e)}")
        return None

def apply_fixes():
    """Apply all CSV parsing fixes to the codebase"""
    fixes_applied = []
    
    # Fix 1: Enhance SFTP pattern matching for CSV files
    try:
        sftp_path = "utils/sftp.py"
        logger.info(f"Applying SFTP pattern matching fix to {sftp_path}")
        
        if not os.path.exists(sftp_path):
            logger.error(f"SFTP file not found at {sftp_path}")
        else:
            backup_path = backup_file(sftp_path)
            
            # Read the file
            with open(sftp_path, 'r') as f:
                content = f.read()
            
            # Fix pattern matching section
            # Add support for more date/time formats in filename matching
            if "csv_patterns =" in content:
                # Find the pattern list definition
                pattern_start = content.find("csv_patterns =")
                if pattern_start > 0:
                    pattern_end = content.find("]", pattern_start)
                    if pattern_end > 0:
                        old_patterns = content[pattern_start:pattern_end+1]
                        
                        # Create enhanced patterns
                        new_patterns = """csv_patterns = [
            # Primary pattern - matches YYYY.MM.DD-HH.MM.SS.csv (Emeralds Killfeed standard)
            r'\\d{4}\\.\\d{2}\\.\\d{2}-\\d{2}\\.\\d{2}\\.\\d{2}\\.csv$',
            
            # Variant with colons instead of dots in the time portion
            r'\\d{4}\\.\\d{2}\\.\\d{2}-\\d{2}:\\d{2}:\\d{2}\\.csv$',
            
            # Variant with space instead of dash between date and time
            r'\\d{4}\\.\\d{2}\\.\\d{2} \\d{2}\\.\\d{2}\\.\\d{2}\\.csv$',
            r'\\d{4}\\.\\d{2}\\.\\d{2} \\d{2}:\\d{2}:\\d{2}\\.csv$',
            
            # ISO date format variants
            r'\\d{4}-\\d{2}-\\d{2}-\\d{2}\\.\\d{2}\\.\\d{2}\\.csv$',  # YYYY-MM-DD-HH.MM.SS.csv
            r'\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.csv$',    # YYYY-MM-DD HH:MM:SS.csv
            
            # Alternative patterns - match various date formats without full time
            r'\\d{4}\\.\\d{2}\\.\\d{2}.*\\.csv$',    # YYYY.MM.DD*.csv (any time format)
            r'\\d{4}-\\d{2}-\\d{2}.*\\.csv$',      # YYYY-MM-DD*.csv (ISO format)
            
            # Date in different position variants
            r'.*\\d{4}\\.\\d{2}\\.\\d{2}.*\\.csv$',  # Any prefix with YYYY.MM.DD*.csv
            
            # Generic CSV fallback as last resort
            r'.*\\.csv$'
        ]"""
                        
                        # Replace patterns
                        new_content = content[:pattern_start] + new_patterns + content[pattern_end+1:]
                        
                        # Write updated file
                        with open(sftp_path, 'w') as f:
                            f.write(new_content)
                        
                        logger.info(f"Successfully enhanced CSV filename patterns in {sftp_path}")
                        fixes_applied.append("SFTP pattern matching")
                    else:
                        logger.error("Could not find end of pattern list")
                else:
                    logger.error("Could not find pattern list in SFTP file")
            else:
                logger.warning(f"Pattern list not found in {sftp_path}")
    except Exception as e:
        logger.error(f"Error applying SFTP pattern fix: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    # Fix 2: Enhance delimiter detection in CSV parser
    try:
        csv_parser_path = "utils/csv_parser.py"
        logger.info(f"Applying enhanced delimiter detection to {csv_parser_path}")
        
        if not os.path.exists(csv_parser_path):
            logger.error(f"CSV parser file not found at {csv_parser_path}")
        else:
            backup_path = backup_file(csv_parser_path)
            
            # Read the file
            with open(csv_parser_path, 'r') as f:
                content = f.read()
            
            # Find delimiter detection section
            delimiter_section_start = content.find("# Count occurrences of potential delimiters")
            if delimiter_section_start > 0:
                # Find the end of this section (next major section)
                next_section = content.find("# Create CSV reader", delimiter_section_start)
                if next_section > 0:
                    old_delimiter_code = content[delimiter_section_start:next_section]
                    
                    # Enhanced delimiter detection code
                    new_delimiter_code = """        # Count occurrences of potential delimiters
        delimiters = {';': 0, ',': 0, '\\t': 0, '|': 0}  # Added pipe as another possible delimiter
        for d in delimiters:
            delimiters[d] = file_content.count(d)
        
        # Add extra weight to semicolons to handle mixed format files better
        # Game logs commonly use semicolons and we want to prioritize them
        if delimiters.get(';', 0) > 0:
            original_count = delimiters[';']
            delimiters[';'] *= 2.0  # Boost semicolons by 100% in detection (increased from 50%)
            logger.debug(f"Boosting semicolon count from {original_count} to {delimiters[';']}")
        
        # Check for patterns that strongly indicate semicolon delimiter
        # Look for multiple sequential semicolons which often indicate empty fields in semicolon-delimited files
        if ';;' in file_content or ';;;' in file_content:
            logger.debug("Found multiple sequential semicolons, strongly indicates semicolon delimiter")
            delimiters[';'] += 50  # Add a strong bonus for this pattern
        
        # Check for patterns that indicate comma delimiter
        # In comma-delimited files, text fields are often quoted
        if file_content.count('","') > 5:
            logger.debug("Found quoted comma patterns, indicates comma delimiter")
            delimiters[','] += 20
        
        # Sample line analysis for delimiter determination
        try:
            sample_lines = file_content.split('\\n')[:10]  # Analyze more lines
            line_scores = {';': 0, ',': 0, '\\t': 0, '|': 0}
            
            for line in sample_lines:
                if not line.strip():
                    continue
                
                # Check which delimiter produces the most consistent field counts
                for d in delimiters.keys():
                    if d in line:
                        fields = line.split(d)
                        # More fields generally means this is the right delimiter
                        if len(fields) > 1:
                            line_scores[d] += len(fields)
            
            # Add these scores to our delimiter counts
            for d, score in line_scores.items():
                if score > 0:
                    logger.debug(f"Adding line analysis score for '{d}': {score}")
                    delimiters[d] += score
        except Exception as e:
            logger.warning(f"Error during line analysis for delimiter detection: {e}")
        
        # Use the most frequent delimiter if it appears significantly
        best_delimiter = self.separator  # Default
        max_count = delimiters.get(self.separator, 0)
        
        for d, count in delimiters.items():
            if count > max_count:
                max_count = count
                best_delimiter = d
"""
                    
                    # Replace with enhanced code
                    new_content = content[:delimiter_section_start] + new_delimiter_code + content[next_section:]
                    
                    # Write updated file
                    with open(csv_parser_path, 'w') as f:
                        f.write(new_content)
                    
                    logger.info(f"Successfully enhanced delimiter detection in {csv_parser_path}")
                    fixes_applied.append("Delimiter detection")
                else:
                    logger.error("Could not find end of delimiter detection section")
            else:
                logger.error("Could not find delimiter detection section in CSV parser")
    except Exception as e:
        logger.error(f"Error applying delimiter detection fix: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    # Fix 3: Enhance timestamp parsing with multiple formats
    try:
        csv_parser_path = "utils/csv_parser.py"
        logger.info(f"Applying enhanced timestamp parsing to {csv_parser_path}")
        
        if not os.path.exists(csv_parser_path):
            logger.error(f"CSV parser file not found at {csv_parser_path}")
        else:
            # Backup is not needed here as it was already created in the previous step
            backup_path = None
            
            # Read the file
            with open(csv_parser_path, 'r') as f:
                content = f.read()
            
            # Find the timestamp format section
            timestamp_section_start = content.find("# Try these common formats")
            if timestamp_section_start > 0:
                # Find the formats list
                formats_end = content.find("for fmt in alternative_formats:", timestamp_section_start)
                if formats_end > 0:
                    old_formats = content[timestamp_section_start:formats_end]
                    
                    # Enhanced timestamp formats
                    new_formats = """                        # Try these common formats
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
                    
                    # Replace with enhanced formats
                    new_content = content[:timestamp_section_start] + new_formats + content[formats_end:]
                    
                    # Write updated file
                    with open(csv_parser_path, 'w') as f:
                        f.write(new_content)
                    
                    logger.info(f"Successfully enhanced timestamp parsing in {csv_parser_path}")
                    fixes_applied.append("Timestamp parsing")
                else:
                    logger.error("Could not find end of timestamp formats section")
            else:
                logger.error("Could not find timestamp formats section in CSV parser")
    except Exception as e:
        logger.error(f"Error applying timestamp parsing fix: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    # Fix 4: Improve empty file handling
    try:
        csv_parser_path = "utils/csv_parser.py"
        logger.info(f"Applying improved empty file handling to {csv_parser_path}")
        
        if not os.path.exists(csv_parser_path):
            logger.error(f"CSV parser file not found at {csv_parser_path}")
        else:
            # Read the file (use existing backup from previous step)
            with open(csv_parser_path, 'r') as f:
                content = f.read()
            
            # Find the empty file check section
            empty_check_start = content.find("# If file is empty or just whitespace")
            if empty_check_start > 0:
                # Find the end of the section (next section)
                empty_check_end = content.find("# Count occurrences", empty_check_start)
                if empty_check_end > 0:
                    old_empty_check = content[empty_check_start:empty_check_end]
                    
                    # Enhanced empty file detection
                    new_empty_check = """        # Enhanced empty file detection
        # Check multiple conditions to properly detect empty files
        is_empty = False
        
        # Check if content is None or empty string
        if file_content is None or file_content == '':
            is_empty = True
            logger.warning(f"Empty CSV file detected (no content): {file_path or 'unknown'}")
        # Check if content contains only whitespace
        elif isinstance(file_content, str) and not file_content.strip():
            is_empty = True
            logger.warning(f"Empty CSV file detected (only whitespace): {file_path or 'unknown'}")
        # Check if content is too short to be meaningful (less than 10 chars)
        elif isinstance(file_content, str) and len(file_content.strip()) < 10:
            logger.warning(f"Nearly empty CSV file detected (less than 10 chars): {file_path or 'unknown'}")
            # Log the actual content for debugging
            logger.debug(f"File content: '{file_content}'")
            
            # Check if short content is actually a valid row
            if (',' in file_content or ';' in file_content) and len(file_content.strip().split(',')) >= 3:
                logger.info(f"Short content appears to be a valid CSV row, will attempt to parse")
            else:
                is_empty = True
        # For binary content, check length
        elif isinstance(file_content, bytes) and len(file_content) < 10:
            is_empty = True
            logger.warning(f"Empty binary CSV file detected (less than 10 bytes): {file_path or 'unknown'}")
            
        if is_empty:
            logger.info(f"Skipping empty file: {file_path or 'unknown'}")
            return []
            
"""
                    
                    # Replace with enhanced empty file check
                    new_content = content[:empty_check_start] + new_empty_check + content[empty_check_end:]
                    
                    # Write updated file
                    with open(csv_parser_path, 'w') as f:
                        f.write(new_content)
                    
                    logger.info(f"Successfully enhanced empty file handling in {csv_parser_path}")
                    fixes_applied.append("Empty file handling")
                else:
                    logger.error("Could not find end of empty file check section")
            else:
                logger.error("Could not find empty file check section in CSV parser")
    except Exception as e:
        logger.error(f"Error applying empty file handling fix: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    # Fix 5: Improve row validation for incomplete rows
    try:
        csv_parser_path = "utils/csv_parser.py"
        logger.info(f"Applying improved row validation to {csv_parser_path}")
        
        if not os.path.exists(csv_parser_path):
            logger.error(f"CSV parser file not found at {csv_parser_path}")
        else:
            # Read the file (use existing backup from previous step)
            with open(csv_parser_path, 'r') as f:
                content = f.read()
            
            # Find the row validation section
            row_validation_start = content.find("# More permissive handling of rows with insufficient fields")
            if row_validation_start > 0:
                # Find the end of the basic validation
                row_validation_end = content.find("logger.debug(f\"Processing row {current_line}", row_validation_start)
                if row_validation_end > 0:
                    old_validation = content[row_validation_start:row_validation_end]
                    
                    # Enhanced row validation
                    new_validation = """                # More permissive handling of rows with insufficient fields
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
                    
                    # Replace with enhanced row validation
                    new_content = content[:row_validation_start] + new_validation + content[row_validation_end:]
                    
                    # Write updated file
                    with open(csv_parser_path, 'w') as f:
                        f.write(new_content)
                    
                    logger.info(f"Successfully enhanced row validation in {csv_parser_path}")
                    fixes_applied.append("Row validation")
                else:
                    logger.error("Could not find end of row validation section")
            else:
                logger.error("Could not find row validation section in CSV parser")
    except Exception as e:
        logger.error(f"Error applying row validation fix: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    # Report on applied fixes
    if fixes_applied:
        logger.info(f"Successfully applied {len(fixes_applied)} fixes: {', '.join(fixes_applied)}")
        return True
    else:
        logger.warning("No fixes were applied")
        return False

def run_tests():
    """Run tests for CSV parser with sample files"""
    logger.info("Running tests on CSV parser with sample files")
    
    # First make sure our test script is available
    test_script = "test_all_csv_files.py"
    if not os.path.exists(test_script):
        logger.error(f"Test script not found: {test_script}")
        return False
    
    # Run the test script
    logger.info(f"Executing test script: {test_script}")
    import subprocess
    try:
        result = subprocess.run([sys.executable, test_script], 
                                capture_output=True, text=True, check=True)
        logger.info("Test execution output:")
        for line in result.stdout.splitlines():
            logger.info(f"  {line}")
        
        logger.info("Tests completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Test execution failed with code {e.returncode}:")
        logger.error(e.stderr)
        return False
    except Exception as e:
        logger.error(f"Error running tests: {str(e)}")
        return False

def main():
    """Main function to apply fixes and run tests"""
    parser = argparse.ArgumentParser(description="Integrated CSV Fix for Tower of Temptation Discord Bot")
    parser.add_argument("--apply", action="store_true", help="Apply the fixes to the main codebase")
    parser.add_argument("--test", action="store_true", help="Run tests on the sample CSV files")
    args = parser.parse_args()
    
    if not args.apply and not args.test:
        parser.print_help()
        logger.info("No actions specified. Use --apply to apply fixes or --test to run tests.")
        return
    
    if args.apply:
        logger.info("Applying CSV fixes to codebase...")
        if apply_fixes():
            logger.info("All fixes applied successfully")
        else:
            logger.warning("Some fixes may not have been applied correctly")
    
    if args.test:
        logger.info("Running CSV parser tests...")
        if run_tests():
            logger.info("All tests passed successfully")
        else:
            logger.warning("Some tests failed, check the logs for details")

if __name__ == "__main__":
    main()