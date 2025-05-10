#!/usr/bin/env python3
"""
Comprehensive CSV File Testing Script

This script tests all CSV files in the attached_assets directory
to verify that they can be properly parsed by the CSV parser.
"""
import os
import logging
import json
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('csv_test.log')
    ]
)
logger = logging.getLogger('csv_test')

class CSVTestResults:
    """Store test results for reporting"""
    def __init__(self):
        self.csv_files_found = 0
        self.csv_files_processed = 0
        self.events_processed = 0
        self.empty_files = 0
        self.errors = []
        self.file_results = {}
        self.start_time = datetime.now()
        
    def add_error(self, file_path, error):
        """Add an error to the results"""
        self.errors.append(f"{file_path}: {error}")
        
    def add_file_result(self, file_path, events_count, empty=False, error=None):
        """Add a file result to the collection"""
        self.file_results[file_path] = {
            "events_count": events_count,
            "empty": empty,
            "error": error,
            "success": error is None
        }
        
    def to_dict(self):
        """Convert results to dictionary"""
        return {
            "csv_files_found": self.csv_files_found,
            "csv_files_processed": self.csv_files_processed,
            "events_processed": self.events_processed,
            "empty_files": self.empty_files,
            "errors": self.errors,
            "file_results": self.file_results,
            "start_time": self.start_time.isoformat(),
            "end_time": datetime.now().isoformat(),
            "duration_seconds": (datetime.now() - self.start_time).total_seconds(),
            "success": len(self.errors) == 0
        }
        
    def save_to_file(self, filename="csv_test_results.json"):
        """Save results to JSON file"""
        with open(filename, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
            
    def print_summary(self):
        """Print a summary of the test results"""
        success = len(self.errors) == 0
        print("\n========================================")
        print(f"CSV Processing Test Results: {'SUCCESS' if success else 'FAILURE'}")
        print("========================================")
        print(f"CSV Files Found:     {self.csv_files_found}")
        print(f"CSV Files Processed: {self.csv_files_processed}")
        print(f"Events Processed:    {self.events_processed}")
        print(f"Empty Files:         {self.empty_files}")
        print(f"Errors:              {len(self.errors)}")
        
        print("\nFile Results:")
        for file_path, result in self.file_results.items():
            status = "SUCCESS" if result["success"] else "FAILED"
            events = result["events_count"]
            empty = " (empty)" if result["empty"] else ""
            print(f"  {os.path.basename(file_path)}: {status} - {events} events{empty}")
            
        if self.errors:
            print("\nErrors encountered:")
            for i, error in enumerate(self.errors, 1):
                print(f"  {i}. {error}")
        print("========================================")

def test_all_csv_files():
    """Test all CSV files in the attached_assets directory"""
    logger.info("Starting CSV file test")
    
    results = CSVTestResults()
    
    try:
        # Import the CSV parser
        try:
            from utils.csv_parser import CSVParser
        except ImportError:
            logger.error("Failed to import CSVParser - check module structure")
            raise
        
        # Find all CSV files in the attached_assets directory
        csv_files = [f for f in os.listdir('attached_assets') if f.endswith('.csv')]
        results.csv_files_found = len(csv_files)
        logger.info(f"Found {len(csv_files)} CSV files in attached_assets directory")
        
        # Process each CSV file
        csv_parser = CSVParser()
        for csv_file in csv_files:
            file_path = os.path.join('attached_assets', csv_file)
            logger.info(f"Testing {file_path}")
            
            try:
                with open(file_path, 'r') as f:
                    csv_data = f.read()
                
                if not csv_data.strip():
                    logger.warning(f"{file_path} is empty")
                    results.empty_files += 1
                    results.csv_files_processed += 1
                    results.add_file_result(file_path, 0, empty=True)
                    continue
                
                events = csv_parser.parse_csv_data(csv_data, file_path)
                logger.info(f"Parsed {len(events)} events from {file_path}")
                
                results.csv_files_processed += 1
                results.events_processed += len(events)
                results.add_file_result(file_path, len(events))
                
                # Log sample event for verification
                if events:
                    logger.info(f"Sample event: {events[0]}")
                else:
                    # If no events but file is not empty, mark as empty
                    logger.warning(f"{file_path} has content but no events were parsed")
                    results.empty_files += 1
                    results.add_file_result(file_path, 0, empty=True)
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error processing {file_path}: {error_msg}")
                results.add_error(file_path, error_msg)
                results.add_file_result(file_path, 0, error=error_msg)
    
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Unexpected error in test: {error_msg}")
        results.add_error("general", error_msg)
    
    # Save and print results
    results.save_to_file()
    results.print_summary()
    return results

if __name__ == "__main__":
    test_all_csv_files()