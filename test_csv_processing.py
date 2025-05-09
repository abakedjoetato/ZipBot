#!/usr/bin/env python3
"""
CSV Processing Verification Script

This script tests all the CSV parsing and processing enhancements to ensure they're working correctly.
It processes a directory of sample CSV files and reports the results.
"""

import os
import logging
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('csv_processor_test')

# Add parent directory to path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import our CSV parser and related utilities
try:
    from utils.csv_parser import CSVParser
    logger.info("Successfully imported CSVParser")
except ImportError as e:
    logger.error(f"Error importing CSVParser: {e}")
    sys.exit(1)

def test_csv_file(file_path: str) -> Dict[str, Any]:
    """
    Test a single CSV file and return results.
    
    Args:
        file_path: Path to the CSV file to test
        
    Returns:
        Dict with test results including success status, event count, and error message if any
    """
    result = {
        "file_path": file_path,
        "success": False,
        "event_count": 0,
        "error": None,
        "empty_file": False,
        "events": [],
        "delimiter_detected": None,
        "timestamp_formats_tried": []
    }
    
    try:
        # Initialize the parser
        parser = CSVParser(format_name="deadside")
        
        # Process the file
        logger.info(f"Processing file: {file_path}")
        events = parser.process_csv_file(file_path)
        
        # Store results
        result["success"] = True
        result["event_count"] = len(events)
        result["events"] = events[:5]  # Store up to 5 sample events
        result["delimiter_detected"] = parser.last_detected_delimiter
        
        # Check if this was an empty file
        if len(events) == 0:
            logger.info(f"File {file_path} was processed successfully but contained no events (empty file)")
            result["empty_file"] = True
        else:
            logger.info(f"Successfully processed {len(events)} events from {file_path}")
            
            # Show sample events
            for i, event in enumerate(events[:3]):
                logger.info(f"Event {i+1}: {event}")
        
    except Exception as e:
        logger.error(f"Error processing {file_path}: {str(e)}")
        result["error"] = str(e)
        import traceback
        logger.error(traceback.format_exc())
    
    return result

def test_directory(directory: str) -> Dict[str, Any]:
    """
    Test all CSV files in a directory.
    
    Args:
        directory: Directory containing CSV files
        
    Returns:
        Dict with overall test results
    """
    results = {
        "total_files": 0,
        "successful_files": 0,
        "empty_files": 0,
        "failed_files": 0,
        "total_events": 0,
        "file_results": []
    }
    
    # Find all CSV files in the directory
    csv_files = []
    for file in os.listdir(directory):
        if file.endswith(".csv"):
            csv_files.append(os.path.join(directory, file))
    
    results["total_files"] = len(csv_files)
    logger.info(f"Found {len(csv_files)} CSV files in {directory}")
    
    # Process each file
    for file_path in csv_files:
        file_result = test_csv_file(file_path)
        results["file_results"].append(file_result)
        
        if file_result["success"]:
            results["successful_files"] += 1
            results["total_events"] += file_result["event_count"]
            
            if file_result["empty_file"]:
                results["empty_files"] += 1
        else:
            results["failed_files"] += 1
    
    # Log summary
    logger.info("\n" + "="*80)
    logger.info("SUMMARY OF CSV PROCESSING TESTS")
    logger.info("="*80)
    logger.info(f"Total files tested: {results['total_files']}")
    logger.info(f"Successfully parsed: {results['successful_files']} files")
    logger.info(f"Empty files handled: {results['empty_files']} files")
    logger.info(f"Parsing errors: {results['failed_files']} files")
    logger.info(f"Total events extracted: {results['total_events']} events")
    logger.info("\nDetailed Results:")
    
    for result in results["file_results"]:
        file_name = os.path.basename(result["file_path"])
        if result["success"]:
            if result["empty_file"]:
                logger.info(f"[SUCCESS] {result['file_path']}: 0 events (empty file)")
            else:
                logger.info(f"[SUCCESS] {result['file_path']}: {result['event_count']} events")
        else:
            logger.info(f"[FAILED] {result['file_path']}: {result['error']}")
    
    return results

def test_specific_features():
    """Test specific CSV processing features"""
    logger.info("Testing specific CSV processing features...")
    
    # Test 1: Improved delimiter detection
    test_content = "2025.05.09-11.58.37;TestKiller;12345;TestVictim;67890;AK47;100;PC"
    parser = CSVParser(format_name="deadside")
    delimiter = parser._detect_delimiter(test_content)
    logger.info(f"Delimiter detection test: Detected '{delimiter}' (expected ';')")
    
    # Test 2: Timestamp parsing with different formats
    test_timestamps = [
        "2025.05.09-11.58.37",  # Standard format
        "2025-05-09 11:58:37",  # ISO-style format
        "2025/05/09 11:58:37",  # Slash format
        "09.05.2025-11.58.37",  # European format
        "20250509-115837"       # Compact format
    ]
    
    for ts in test_timestamps:
        try:
            parser = CSVParser(format_name="deadside")
            event = {"timestamp": ts}
            parsed = parser._parse_timestamp(event, "timestamp")
            logger.info(f"Timestamp parsing test: Successfully parsed '{ts}' to {parsed}")
        except Exception as e:
            logger.error(f"Timestamp parsing test: Failed to parse '{ts}': {str(e)}")
    
    # Test 3: Empty file handling
    empty_content = ""
    whitespace_content = "   \n   \n"
    short_content = "a,b,c"
    
    parser = CSVParser(format_name="deadside")
    logger.info(f"Empty file test: Empty string detected as empty? {parser._is_empty_file(empty_content)}")
    logger.info(f"Empty file test: Whitespace detected as empty? {parser._is_empty_file(whitespace_content)}")
    logger.info(f"Empty file test: Short content detected as empty? {parser._is_empty_file(short_content)}")
    
    # Test 4: Row validation with incomplete rows
    short_row = ["2025.05.09-11.58.37", "Player1", "Player2"]
    parser = CSVParser(format_name="deadside")
    try:
        expanded_row = parser._expand_incomplete_row(short_row)
        logger.info(f"Row validation test: Successfully expanded {short_row} to {expanded_row}")
    except Exception as e:
        logger.error(f"Row validation test: Failed to expand short row: {str(e)}")

def main():
    """Main entry point"""
    logger.info("Starting CSV processing tests")
    
    # Test specific features
    test_specific_features()
    
    # Test all CSV files in the 'attached_assets' directory
    result = test_directory("attached_assets")
    
    # Exit with appropriate status code
    if result["failed_files"] > 0:
        logger.error(f"Tests completed with {result['failed_files']} failures")
        sys.exit(1)
    else:
        logger.info("All tests completed successfully!")
        sys.exit(0)

if __name__ == "__main__":
    main()