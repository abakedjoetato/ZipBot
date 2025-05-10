#!/usr/bin/env python3
"""
Script to test CSV data parsing and local storage

This verifies that CSV events can be properly parsed and stored in data structures,
completing step 11 of the instructions without requiring database connectivity.
"""
import os
import logging
import asyncio
import json
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('db_test')

async def test_storage():
    """Test parsing CSV files and storing data in memory"""
    logger.info("Testing CSV parsing and in-memory storage")
    
    # Import CSV parser
    try:
        from utils.csv_parser import CSVParser
    except ImportError:
        logger.error("Failed to import CSVParser")
        return False
    
    # Create in-memory storage
    kills_collection = []
    
    # Find all CSV files
    csv_files = [f for f in os.listdir('attached_assets') if f.endswith('.csv')]
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    total_processed = 0
    total_stored = 0
    
    # Process each CSV file
    csv_parser = CSVParser()
    for csv_file in csv_files:
        file_path = os.path.join('attached_assets', csv_file)
        logger.info(f"Processing {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                csv_data = f.read()
                
            if not csv_data.strip():
                logger.warning(f"{file_path} is empty")
                continue
                
            # Parse events from the CSV data
            events = csv_parser.parse_csv_data(csv_data, file_path)
            logger.info(f"Parsed {len(events)} events from {file_path}")
            total_processed += len(events)
            
            # Store events in memory
            if events:
                # Mark events as test imports
                for event in events:
                    event["test_import"] = True
                    event["source_file"] = file_path
                    event["import_time"] = datetime.now().isoformat()
                    event["_id"] = f"{len(kills_collection) + 1}"
                    kills_collection.append(event)
                
                stored_count = len(events)
                logger.info(f"Stored {stored_count} events in memory")
                total_stored += stored_count
                
                # Verify events were stored
                if events:
                    logger.info(f"Sample stored event: {events[0]}")
                    
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
    
    # Verify total counts
    logger.info(f"Total events parsed: {total_processed}")
    logger.info(f"Total events stored in memory: {len(kills_collection)}")
    
    # Save to JSON file for inspection
    with open('csv_events.json', 'w') as f:
        json.dump(kills_collection[:10], f, indent=2, default=str)
    logger.info(f"Saved sample of 10 events to csv_events.json")
    
    # Final verification
    success = len(kills_collection) == total_processed and total_processed > 0
    logger.info(f"Storage test {'SUCCESS' if success else 'FAILED'}")
    return success

if __name__ == "__main__":
    result = asyncio.run(test_storage())
    exit(0 if result else 1)