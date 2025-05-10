#!/usr/bin/env python3
"""
Direct CSV Test Script

This standalone script bypasses the entire bot infrastructure to directly test CSV parsing
with comprehensive diagnostics at each step.
"""
import os
import sys
import re
import io
import csv
import logging
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('csv_test.log')
    ]
)

logger = logging.getLogger('direct_csv_test')

# Directory to search for CSV files
TEST_DIRS = ['./attached_assets']

# Flag to control output verbosity
VERBOSE = True

def log_memory_usage():
    """Log current memory usage"""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        logger.info(f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")
    except ImportError:
        logger.warning("psutil not installed, cannot log memory usage")

def direct_parse_csv(file_path: str) -> List[Dict[str, Any]]:
    """
    Parse a CSV file directly without any complex infrastructure
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        List of parsed event dictionaries
    """
    logger.info(f"START DIRECT PARSE: {file_path}")
    
    try:
        # Step 1: Read file as binary
        logger.debug(f"Step 1: Reading file as binary: {file_path}")
        with open(file_path, 'rb') as f:
            content = f.read()
        
        logger.debug(f"File size: {len(content)} bytes")
        if len(content) == 0:
            logger.error(f"Empty file: {file_path}")
            return []
        
        # Log first 200 bytes for diagnostics
        logger.debug(f"First 200 bytes: {content[:200]}")
        
        # Step 2: Convert to string
        logger.debug("Step 2: Converting to string")
        try:
            content_str = content.decode('utf-8', errors='replace')
            logger.debug("Successfully decoded as UTF-8")
        except UnicodeDecodeError:
            try:
                logger.debug("UTF-8 decode failed, trying latin-1")
                content_str = content.decode('latin-1')
                logger.debug("Successfully decoded as latin-1")
            except Exception as e:
                logger.error(f"Failed to decode content: {e}")
                return []
        
        # Log first 200 chars for diagnostics
        logger.debug(f"First 200 chars: {content_str[:200]}")
        
        # Count lines
        line_count = content_str.count('\n')
        logger.debug(f"File contains approximately {line_count + 1} lines")
        
        # Step 3: Determine delimiter
        logger.debug("Step 3: Detecting delimiter")
        semicolons = content_str.count(';')
        commas = content_str.count(',')
        tabs = content_str.count('\t')
        
        logger.debug(f"Delimiter counts - semicolons: {semicolons}, commas: {commas}, tabs: {tabs}")
        
        # Choose delimiter by highest count
        delimiter = ';'  # Default
        if commas > semicolons * 2 and commas > tabs:
            delimiter = ','
        elif tabs > semicolons * 2 and tabs > commas:
            delimiter = '\t'
            
        logger.debug(f"Selected delimiter: '{delimiter}'")
        
        # Step 4: Parse with CSV reader
        logger.debug("Step 4: Creating CSV reader")
        string_io = io.StringIO(content_str)
        csv_reader = csv.reader(string_io, delimiter=delimiter)
        
        # Step 5: Process rows
        logger.debug("Step 5: Processing rows")
        events = []
        row_count = 0
        error_count = 0
        success_count = 0
        
        # First check if it might have headers
        has_header = False
        try:
            first_row = next(csv_reader, None)
            if first_row and len(first_row) > 0:
                # Check if first cell looks like a header
                if first_row[0].lower() in ['timestamp', 'date', 'time', 'datetime']:
                    has_header = True
                    logger.debug(f"Detected header row: {first_row}")
                else:
                    # Reset reader to start
                    string_io.seek(0)
                    csv_reader = csv.reader(string_io, delimiter=delimiter)
                    logger.debug(f"First row not a header: {first_row}")
            else:
                logger.warning("Empty first row or empty file")
        except Exception as e:
            logger.error(f"Error checking header: {e}")
            # Reset reader to ensure we start from the beginning
            string_io.seek(0)
            csv_reader = csv.reader(string_io, delimiter=delimiter)
        
        # Process all rows
        for row in csv_reader:
            row_count += 1
            
            # Skip empty rows
            if not row or len(row) < 3:
                logger.debug(f"Skipping empty/short row {row_count}: {row}")
                continue
            
            try:
                # Create event dict based on position
                event = {}
                
                # Standardize row based on field count
                if len(row) >= 7:
                    # Standard format
                    event['timestamp'] = row[0] if len(row) > 0 else ""
                    event['killer_name'] = row[1] if len(row) > 1 else ""
                    event['killer_id'] = row[2] if len(row) > 2 else ""
                    event['victim_name'] = row[3] if len(row) > 3 else ""
                    event['victim_id'] = row[4] if len(row) > 4 else ""
                    event['weapon'] = row[5] if len(row) > 5 else ""
                    event['distance'] = row[6] if len(row) > 6 else "0"
                else:
                    # Best-effort for shorter rows
                    event['timestamp'] = row[0] if len(row) > 0 else datetime.now().strftime("%Y.%m.%d-%H.%M.%S")
                    
                    if len(row) == 3:  # Likely killer, victim, weapon
                        event['killer_name'] = row[0]
                        event['victim_name'] = row[1]
                        event['weapon'] = row[2]
                    elif len(row) == 4:  # Timestamp, killer, victim, weapon
                        event['killer_name'] = row[1]
                        event['victim_name'] = row[2]
                        event['weapon'] = row[3]
                    # Add other cases as needed
                
                # Add to events list
                events.append(event)
                success_count += 1
                
                # Log verbose details for first few events
                if VERBOSE and success_count <= 5:
                    logger.debug(f"Parsed event {success_count}: {event}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing row {row_count}: {e}")
                if error_count <= 5:  # Limit error logs to avoid spam
                    logger.error(f"Problem row: {row}")
        
        logger.info(f"Parsing complete: {success_count} events parsed, {error_count} errors out of {row_count} rows")
        log_memory_usage()
        return events
        
    except Exception as e:
        logger.error(f"Failed to process file {file_path}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

def find_csv_files() -> List[str]:
    """Find all CSV files in test directories"""
    logger.info("Searching for CSV files...")
    csv_files = []
    
    for directory in TEST_DIRS:
        if not os.path.exists(directory):
            logger.warning(f"Directory does not exist: {directory}")
            continue
            
        logger.info(f"Scanning directory: {directory}")
        try:
            for filename in os.listdir(directory):
                if filename.endswith(".csv"):
                    full_path = os.path.join(directory, filename)
                    csv_files.append(full_path)
                    logger.info(f"Found CSV file: {full_path}")
        except Exception as e:
            logger.error(f"Error scanning directory {directory}: {e}")
    
    logger.info(f"Found {len(csv_files)} CSV files total")
    return csv_files

def main():
    """Main function to run the test"""
    logger.info("=== DIRECT CSV TEST SCRIPT STARTING ===")
    log_memory_usage()
    
    # Find CSV files
    csv_files = find_csv_files()
    if not csv_files:
        logger.error("No CSV files found!")
        return
    
    # Process each file
    for file_path in csv_files:
        logger.info(f"Processing file: {file_path}")
        events = direct_parse_csv(file_path)
        logger.info(f"Processed {len(events)} events from {file_path}")
        
        # List first few events for debugging
        if events:
            for i, event in enumerate(events[:5]):
                logger.info(f"Event {i+1}: {event}")
    
    logger.info("=== DIRECT CSV TEST SCRIPT COMPLETE ===")
    log_memory_usage()

if __name__ == "__main__":
    main()