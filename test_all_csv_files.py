"""
Test CSV Parsing for All Sample Files

This script tests the CSV parser with all sample files from the attached_assets directory.
It verifies that our fixes allow successful parsing of all supported CSV formats.
"""

import os
import sys
import logging
import glob
from datetime import datetime
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

async def test_all_csv_files():
    """Test CSV parsing with all sample files"""
    
    # Import parser
    sys.path.append('.')
    
    try:
        from utils.csv_parser import CSVParser
    except ImportError as e:
        logger.error(f"Failed to import CSVParser: {e}")
        return False
    
    # Find all CSV files in attached_assets
    csv_files = glob.glob('attached_assets/*.csv')
    
    if not csv_files:
        logger.error("No CSV files found in attached_assets directory")
        return False
    
    logger.info(f"Found {len(csv_files)} CSV files: {csv_files}")
    
    # Initialize parser
    parser = CSVParser()
    
    # Track results
    results = {
        "total_files": len(csv_files),
        "successful_files": 0,
        "failed_files": 0,
        "total_events": 0,
        "timestamp_success": 0,
        "timestamp_failure": 0,
        "files": {}
    }
    
    # Test each file
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        logger.info(f"Testing file: {file_path}")
        
        try:
            # Check file size
            file_size = os.path.getsize(file_path)
            
            if file_size == 0:
                logger.info(f"File {file_name} is empty (0 bytes) - ignoring")
                results["successful_files"] += 1  # Count empty files as successful since they're properly detected
                results["files"][file_name] = {
                    "success": True,
                    "events": 0,
                    "empty": True
                }
                continue
                
            # Read file content
            with open(file_path, 'r') as f:
                content = f.read()
                
            # Sample first 200 chars
            content_sample = content[:200] + "..." if len(content) > 200 else content
            logger.info(f"File content sample: {content_sample}")
            
            # Test parsing with auto-detection (default behavior)
            events = parser.parse_csv_data(content)
            
            if events:
                results["successful_files"] += 1
                results["total_events"] += len(events)
                
                # Check timestamp parsing
                timestamp_successes = 0
                timestamp_failures = 0
                
                for event in events:
                    timestamp = event.get('timestamp')
                    if isinstance(timestamp, datetime):
                        timestamp_successes += 1
                    else:
                        timestamp_failures += 1
                
                results["timestamp_success"] += timestamp_successes
                results["timestamp_failure"] += timestamp_failures
                
                # Record file results
                results["files"][file_name] = {
                    "success": True,
                    "events": len(events),
                    "timestamp_success": timestamp_successes,
                    "timestamp_failure": timestamp_failures,
                    "sample_event": str(events[0])[:150] + "..." if events else "None"
                }
                
                logger.info(f"Successfully parsed {len(events)} events from {file_name}")
                logger.info(f"Sample event: {str(events[0])[:150]}...")
            else:
                results["failed_files"] += 1
                results["files"][file_name] = {
                    "success": False,
                    "error": "No events parsed"
                }
                logger.warning(f"Failed to parse any events from {file_name}")
        
        except Exception as e:
            results["failed_files"] += 1
            results["files"][file_name] = {
                "success": False,
                "error": str(e)
            }
            logger.error(f"Error parsing {file_name}: {e}")
    
    # Print summary
    logger.info("\n=== CSV Parsing Test Summary ===")
    logger.info(f"Files tested: {results['total_files']}")
    logger.info(f"Successfully parsed: {results['successful_files']}")
    logger.info(f"Failed to parse: {results['failed_files']}")
    logger.info(f"Total events: {results['total_events']}")
    logger.info(f"Timestamp success: {results['timestamp_success']}")
    logger.info(f"Timestamp failure: {results['timestamp_failure']}")
    
    # Print details for failed files
    if results['failed_files'] > 0:
        logger.info("\n=== Failed Files Details ===")
        for file_name, file_result in results['files'].items():
            if not file_result.get('success'):
                logger.info(f"{file_name}: {file_result.get('error')}")
    
    if results['failed_files'] == 0:
        logger.info("✅ All CSV files parsed successfully!")
        return True
    else:
        logger.warning(f"⚠️ {results['failed_files']} files failed to parse")
        return False

if __name__ == "__main__":
    import asyncio
    success = asyncio.run(test_all_csv_files())
    sys.exit(0 if success else 1)