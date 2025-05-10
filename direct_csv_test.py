#!/usr/bin/env python3
"""
Direct CSV Testing Script

This script tests the CSV processing directly using the test CSV files
from attached_assets and outputs the results.
"""

import os
import sys
import logging
from utils.csv_parser import CSVParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger('direct_csv_test')

def main():
    """Process test CSV files directly"""
    # Initialize the CSV parser
    parser = CSVParser()
    
    # Get list of test files
    csv_files = [f for f in os.listdir('attached_assets') if f.endswith('.csv')]
    logger.info(f"Found {len(csv_files)} CSV files to test")
    
    # Process each file
    total_events = 0
    successful_files = 0
    
    for csv_file in csv_files:
        file_path = os.path.join('attached_assets', csv_file)
        logger.info(f"Processing {file_path}")
        
        try:
            # Read the file
            with open(file_path, 'rb') as f:
                content = f.read()
            
            # Process the CSV data
            events = parser.parse_csv_data(content)
            
            # Log the results
            logger.info(f"Successfully processed {file_path}: {len(events)} events found")
            if events:
                logger.info(f"First event: {events[0]}")
            
            total_events += len(events)
            successful_files += 1
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
    
    # Print summary
    logger.info("=" * 40)
    logger.info(f"CSV Processing Test Results:")
    logger.info(f"Files processed: {successful_files}/{len(csv_files)}")
    logger.info(f"Total events found: {total_events}")
    logger.info("=" * 40)
    
    return 0 if successful_files == len(csv_files) else 1

if __name__ == "__main__":
    sys.exit(main())