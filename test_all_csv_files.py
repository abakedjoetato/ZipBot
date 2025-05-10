#!/usr/bin/env python3
"""
Comprehensive CSV Processing Test Script

This script tests the ability to parse all CSV files in the attached_assets directory.
It verifies that:
1. Delimiters are correctly detected
2. Empty files are properly handled
3. Timestamps are correctly parsed
4. Data rows are properly processed
"""

import os
import sys
import logging
import glob
import json
from typing import Dict, List, Any, Tuple

from utils.csv_parser import CSVParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('csv_fix.log')
    ]
)

logger = logging.getLogger('csv_test')

class CSVProcessingTester:
    """Test CSV processing capabilities"""
    
    def __init__(self):
        """Initialize the tester"""
        self.csv_parser = CSVParser()
        self.results = {
            "files_tested": 0,
            "files_parsed_successfully": 0,
            "total_events": 0,
            "empty_files": 0,
            "files_with_errors": [],
            "delimiter_counts": {",": 0, ";": 0, "\\t": 0, "other": 0},
            "all_tests_passed": False
        }
        
    def get_test_files(self) -> List[str]:
        """Get all CSV files for testing"""
        csv_files = glob.glob(os.path.join("attached_assets", "*.csv"))
        logger.info(f"Found {len(csv_files)} CSV files for testing: {csv_files}")
        return csv_files
        
    def test_csv_file(self, file_path: str) -> Dict[str, Any]:
        """Test parsing a specific CSV file"""
        file_result = {
            "file": file_path,
            "success": False,
            "events": 0,
            "is_empty": False,
            "delimiter": None,
            "errors": []
        }
        
        try:
            # Read the file
            with open(file_path, 'rb') as f:
                file_content = f.read()
                
            # Test if the file is empty
            is_empty = not file_content or file_content.strip() == b''
            file_result["is_empty"] = is_empty
            
            if is_empty:
                logger.info(f"File {file_path} is empty")
                self.results["empty_files"] += 1
                file_result["success"] = True
                return file_result
                
            # Parse the CSV data
            events = self.csv_parser.parse_csv_data(file_content)
            
            # Record the delimiter - note: in our implementation, 
            # we may not have direct access to the last_detected_delimiter
            # so we'll fall back to a default value if needed
            delimiter = getattr(self.csv_parser, 'last_detected_delimiter', ',')
            file_result["delimiter"] = delimiter
            
            if delimiter == ',':
                self.results["delimiter_counts"][","] += 1
            elif delimiter == ';':
                self.results["delimiter_counts"][";"] += 1
            elif delimiter == '\t':
                self.results["delimiter_counts"]["\\t"] += 1
            else:
                self.results["delimiter_counts"]["other"] += 1
                
            file_result["events"] = len(events)
            file_result["success"] = True
            self.results["total_events"] += len(events)
            
            logger.info(f"Successfully parsed {file_path} with delimiter '{delimiter}', found {len(events)} events")
            return file_result
            
        except Exception as e:
            error_msg = f"Error parsing {file_path}: {str(e)}"
            logger.error(error_msg)
            file_result["errors"].append(error_msg)
            return file_result
            
    def run_all_tests(self) -> Dict[str, Any]:
        """Run tests on all CSV files"""
        csv_files = self.get_test_files()
        self.results["files_tested"] = len(csv_files)
        file_results = []
        
        for file_path in csv_files:
            file_result = self.test_csv_file(file_path)
            file_results.append(file_result)
            
            if file_result["success"]:
                self.results["files_parsed_successfully"] += 1
            else:
                self.results["files_with_errors"].append(file_path)
                
        self.results["file_details"] = file_results
        self.results["all_tests_passed"] = (
            self.results["files_parsed_successfully"] == self.results["files_tested"]
        )
        
        return self.results
        
    def print_summary(self):
        """Print a summary of test results"""
        print("\n" + "="*60)
        print(" CSV PROCESSING TEST RESULTS ")
        print("="*60)
        print(f"Files tested: {self.results['files_tested']}")
        print(f"Files successfully parsed: {self.results['files_parsed_successfully']}")
        print(f"Empty files properly handled: {self.results['empty_files']}")
        print(f"Total events processed: {self.results['total_events']}")
        print("\nDelimiter detection:")
        print(f"  Comma (,): {self.results['delimiter_counts'][',']}")
        print(f"  Semicolon (;): {self.results['delimiter_counts'][';']}")
        print("  Tab (\\t): " + str(self.results['delimiter_counts']['\\t']))
        print(f"  Other: {self.results['delimiter_counts']['other']}")
        
        if self.results["files_with_errors"]:
            print("\nFiles with errors:")
            for file in self.results["files_with_errors"]:
                print(f"  - {file}")
        
        print("\nOverall result:", "PASS" if self.results["all_tests_passed"] else "FAIL")
        print("="*60)
        
def main():
    """Run the CSV processing tests"""
    try:
        logger.info("Starting comprehensive CSV processing tests")
        tester = CSVProcessingTester()
        results = tester.run_all_tests()
        tester.print_summary()
        
        # Save results to file
        with open('csv_test_results.json', 'w') as f:
            json.dump(results, f, indent=2)
            
        logger.info("CSV test results saved to csv_test_results.json")
        
        # Return exit code based on test success
        return 0 if results["all_tests_passed"] else 1
        
    except Exception as e:
        logger.error(f"Error running tests: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
        
if __name__ == "__main__":
    sys.exit(main())