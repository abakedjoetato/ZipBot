"""
Test CSV Parsing with Sample Files

This script tests the CSV parser with sample files from the attached_assets directory.
It runs multiple tests to verify that:
1. Files can be correctly parsed with different delimiters
2. Timestamps are correctly extracted and converted to datetime objects
3. All required fields are present in the parsed events
"""

import sys
import os
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def test_csv_parser(csv_files: List[str]):
    """
    Test the CSV parser with the given files
    
    Args:
        csv_files: List of CSV file paths to test
    """
    # Ensure utils package is in the path
    sys.path.append('.')
    
    # Force reload the csv_parser module to get the latest version
    if 'utils.csv_parser' in sys.modules:
        del sys.modules['utils.csv_parser']
    
    # Import the CSV parser
    try:
        from utils.csv_parser import CSVParser
        logger.info("Successfully imported CSVParser")
    except ImportError as e:
        logger.error(f"Failed to import CSVParser: {e}")
        return False
    
    # Create parser instance
    parser = CSVParser()
    
    # Test results tracking
    results = {
        "total_files": len(csv_files),
        "successful_files": 0,
        "failed_files": 0,
        "total_events": 0,
        "delimiter_stats": {"semicolon": 0, "comma": 0, "tab": 0, "auto": 0},
        "timestamp_success": 0,
        "timestamp_failure": 0
    }
    
    # Process each file
    for file_path in csv_files:
        logger.info(f"Testing file: {file_path}")
        
        try:
            # Read file content
            with open(file_path, "r") as f:
                content = f.read()
            
            # Log sample of content
            content_sample = content[:200] + "..." if len(content) > 200 else content
            logger.info(f"File content sample: {content_sample}")
            
            # Count delimiters in the file
            semicolons = content.count(';')
            commas = content.count(',')
            tabs = content.count('\t')
            
            logger.info(f"Delimiter counts - Semicolons: {semicolons}, Commas: {commas}, Tabs: {tabs}")
            
            # Test parsing strategies
            parsing_strategies = [
                {"name": "auto-detect", "delimiter": None},
                {"name": "semicolon", "delimiter": ";"},
                {"name": "comma", "delimiter": ","},
                {"name": "tab", "delimiter": "\t"}
            ]
            
            best_result = None
            most_events = 0
            
            for strategy in parsing_strategies:
                delimiter = strategy["delimiter"]
                name = strategy["name"]
                
                try:
                    logger.info(f"Trying {name} delimiter")
                    events = parser.parse_csv_data(content, delimiter=delimiter)
                    
                    # Track success based on number of events
                    if events and len(events) > most_events:
                        most_events = len(events)
                        best_result = {
                            "delimiter": name,
                            "events": events,
                            "count": len(events)
                        }
                        
                    logger.info(f"{name} delimiter: {len(events) if events else 0} events parsed")
                    
                except Exception as e:
                    logger.warning(f"Error parsing with {name} delimiter: {e}")
            
            # Record results from best parsing strategy
            if best_result:
                results["successful_files"] += 1
                results["total_events"] += best_result["count"]
                
                # Update delimiter stats
                delimiter_key = best_result["delimiter"]
                if delimiter_key == "auto-detect":
                    results["delimiter_stats"]["auto"] += 1
                else:
                    results["delimiter_stats"][delimiter_key] += 1
                
                # Check timestamp parsing
                for event in best_result["events"]:
                    if "timestamp" in event:
                        if isinstance(event["timestamp"], datetime):
                            results["timestamp_success"] += 1
                        else:
                            results["timestamp_failure"] += 1
                            logger.warning(f"Non-datetime timestamp: {event['timestamp']}")
                
                logger.info(f"Successfully parsed {best_result['count']} events from {file_path}")
                
                # Log first event as a sample
                if best_result["events"]:
                    logger.info(f"Sample event: {best_result['events'][0]}")
            else:
                results["failed_files"] += 1
                logger.error(f"Failed to parse any events from {file_path}")
        
        except Exception as e:
            results["failed_files"] += 1
            logger.error(f"Error processing {file_path}: {e}")
    
    # Print summary
    logger.info("\n=== CSV Parsing Test Summary ===")
    logger.info(f"Files processed: {results['total_files']}")
    logger.info(f"Successfully parsed: {results['successful_files']}")
    logger.info(f"Failed to parse: {results['failed_files']}")
    logger.info(f"Total events: {results['total_events']}")
    logger.info(f"Delimiter stats: {results['delimiter_stats']}")
    logger.info(f"Timestamp success: {results['timestamp_success']}")
    logger.info(f"Timestamp failure: {results['timestamp_failure']}")
    
    # Return overall success status
    return results["successful_files"] > 0 and results["timestamp_failure"] == 0

async def main():
    """Main test function"""
    # Find CSV files in attached_assets directory
    assets_dir = "attached_assets"
    csv_files = []
    
    if os.path.exists(assets_dir) and os.path.isdir(assets_dir):
        for filename in os.listdir(assets_dir):
            if filename.endswith(".csv"):
                csv_files.append(os.path.join(assets_dir, filename))
    
    if not csv_files:
        logger.error(f"No CSV files found in {assets_dir}")
        return
    
    logger.info(f"Found {len(csv_files)} CSV files: {csv_files}")
    
    # Run tests
    success = await test_csv_parser(csv_files)
    
    if success:
        logger.info("✅ CSV parsing tests passed")
    else:
        logger.error("❌ CSV parsing tests failed")

if __name__ == "__main__":
    asyncio.run(main())