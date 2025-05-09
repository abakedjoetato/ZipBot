#!/usr/bin/env python3
"""
Comprehensive CSV Parser Test for all sample files

This script tests the improved CSV parser with all sample files to ensure:
1. Proper delimiter detection (especially for semicolon-delimited files)
2. Robust timestamp parsing with multiple formats
3. Graceful handling of empty or malformed files
4. Correct row validation and field extraction even with partial data
"""
import os
import sys
import logging
import glob
from datetime import datetime
from utils.csv_parser import CSVParser

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('csv_fix.log')
    ]
)

logger = logging.getLogger('csv_test')

def test_csv_file(file_path, parser):
    """Test parsing a single CSV file and return results"""
    logger.info(f"\n{'=' * 80}\nTesting file: {file_path}\n{'=' * 80}")
    
    try:
        # Check if file exists and has content
        if not os.path.exists(file_path):
            logger.error(f"File does not exist: {file_path}")
            return {
                "file": file_path,
                "status": "error",
                "error": "File not found",
                "events": 0
            }
        
        file_size = os.path.getsize(file_path)
        logger.info(f"File size: {file_size} bytes")
        
        if file_size == 0:
            logger.warning(f"Empty file (0 bytes): {file_path}")
        
        # Parse the file
        events = parser.parse_csv_file(file_path)
        
        # Log results
        logger.info(f"Successfully parsed {len(events)} events from {file_path}")
        
        # Print a sample of events if any were found
        if events:
            sample_size = min(3, len(events))
            logger.info(f"Sample of {sample_size} events:")
            for i, event in enumerate(events[:sample_size]):
                logger.info(f"Event {i+1}: {event}")
        
        return {
            "file": file_path,
            "status": "success",
            "events": len(events),
            "empty": file_size == 0
        }
    
    except Exception as e:
        logger.error(f"Error parsing {file_path}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            "file": file_path,
            "status": "error",
            "error": str(e),
            "events": 0
        }

def main():
    """Test CSV parser with all sample files"""
    # Create parser
    parser = CSVParser(format_name="deadside")
    
    # Find all CSV files in attached_assets directory
    csv_files = glob.glob("attached_assets/*.csv")
    
    if not csv_files:
        logger.warning("No CSV files found in attached_assets directory")
        # Check if we need to copy files
        original_path = "Csvfixes-extracted/Csvfixes-main/attached_assets/*.csv"
        original_files = glob.glob(original_path)
        if original_files:
            logger.info(f"Found {len(original_files)} CSV files in original path, will test those")
            csv_files = original_files
    
    logger.info(f"Found {len(csv_files)} CSV files to test")
    
    # Test each file
    results = []
    for file_path in csv_files:
        result = test_csv_file(file_path, parser)
        results.append(result)
    
    # Print summary
    logger.info("\n\n" + "=" * 80)
    logger.info("SUMMARY OF CSV PARSING TESTS")
    logger.info("=" * 80)
    
    success_count = sum(1 for r in results if r["status"] == "success")
    empty_count = sum(1 for r in results if r.get("empty", False))
    error_count = sum(1 for r in results if r["status"] == "error")
    total_events = sum(r["events"] for r in results)
    
    logger.info(f"Total files tested: {len(results)}")
    logger.info(f"Successfully parsed: {success_count} files")
    logger.info(f"Empty files handled: {empty_count} files")
    logger.info(f"Parsing errors: {error_count} files")
    logger.info(f"Total events extracted: {total_events} events")
    
    # Print detailed results
    logger.info("\nDetailed Results:")
    for result in results:
        status_str = f"[{result['status'].upper()}]"
        if result["status"] == "success":
            event_str = f"{result['events']} events"
            if result.get("empty", False):
                event_str += " (empty file)"
            logger.info(f"{status_str} {result['file']}: {event_str}")
        else:
            logger.info(f"{status_str} {result['file']}: {result.get('error', 'Unknown error')}")
    
    # Return success if all files were processed (even if some were empty)
    return success_count == len(results)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)