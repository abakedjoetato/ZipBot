#!/usr/bin/env python3
"""
Script to test CSV data parsing and database storage

This verifies that CSV events can be properly parsed and stored
in the database, completing step 11 of the instructions.
"""
import os
import logging
import asyncio
import json
from datetime import datetime
from pymongo import MongoClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('db_test')

async def test_database_storage():
    """Test parsing CSV files and storing data in the database"""
    logger.info("Testing database storage of CSV data")
    
    # Initialize MongoDB connection
    client = MongoClient("mongodb://localhost:27017/")
    db = client["tower_of_temptation"]
    kills_collection = db["kills"]
    
    # Import CSV parser
    try:
        from utils.csv_parser import CSVParser
    except ImportError:
        logger.error("Failed to import CSVParser")
        return False
    
    # Clear existing test data
    kills_collection.delete_many({"test_import": True})
    logger.info(f"Cleared existing test data from kills collection")
    
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
            
            # Store events in the database
            if events:
                # Mark events as test imports
                for event in events:
                    event["test_import"] = True
                    event["source_file"] = file_path
                    event["import_time"] = datetime.now()
                
                result = kills_collection.insert_many(events)
                stored_count = len(result.inserted_ids)
                logger.info(f"Stored {stored_count} events in database")
                total_stored += stored_count
                
                # Verify events were stored
                sample_event = events[0]
                stored_event = kills_collection.find_one({"_id": result.inserted_ids[0]})
                if stored_event:
                    logger.info(f"Sample stored event: {stored_event}")
                    
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
    
    # Verify total counts
    db_count = kills_collection.count_documents({"test_import": True})
    logger.info(f"Total events parsed: {total_processed}")
    logger.info(f"Total events stored in database: {db_count}")
    
    # Final verification
    success = db_count == total_processed and total_processed > 0
    logger.info(f"Database storage test {'SUCCESS' if success else 'FAILED'}")
    return success

if __name__ == "__main__":
    result = asyncio.run(test_database_storage())
    exit(0 if result else 1)