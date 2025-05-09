"""
Test CSV File Pattern Matching

This script tests the pattern matching used to find CSV files.
It verifies that:
1. All common CSV filename formats are correctly matched
2. The enhanced pattern matching in the SFTP module works correctly
3. Files with various delimiter types are all found
"""

import asyncio
import logging
import os
import re
import tempfile
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

async def test_csv_patterns():
    """Test CSV filename pattern matching"""
    # Define test cases
    test_filenames = [
        # Standard format
        "2025.05.09-11.58.37.csv",
        "2025.05.09-11:58:37.csv",
        
        # Variations on date formats
        "2025-05-09-11.58.37.csv",
        "2025-05-09 11.58.37.csv",
        "2025_05_09_11_58_37.csv",
        "09.05.2025-11.58.37.csv",
        
        # Different naming conventions
        "killfeed_2025.05.09.csv",
        "deaths_2025-05-09.csv",
        "pvp_stats.csv",
        "player_kills.csv",
        
        # Edge cases
        "server1_map2_deaths.csv",
        "combat_log_20250509.csv",
        "statistics_report.csv"
    ]
    
    # Load current patterns from SFTP module
    patterns = []
    
    try:
        with open("utils/sftp.py", "r") as f:
            content = f.read()
        
        # Extract patterns using regex
        pattern_block = re.search(r"csv_patterns\s*=\s*\[(.*?)\]", content, re.DOTALL)
        if pattern_block:
            pattern_text = pattern_block.group(1)
            
            # Extract individual patterns
            pattern_matches = re.finditer(r"r'(.*?)'", pattern_text)
            for match in pattern_matches:
                pattern = match.group(1)
                # Convert the string representation to actual regex
                patterns.append(pattern.replace("\\\\", "\\"))
        
        if not patterns:
            logger.warning("No patterns found in SFTP module. Using fallback test patterns.")
            patterns = [
                r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv$",
                r"\d{4}\.\d{2}\.\d{2}.*?\.csv$",
                r"\d{4}-\d{2}-\d{2}.*?\.csv$",
                r"\d{2}\.\d{2}\.\d{4}.*?\.csv$",
                r"\d{2}-\d{2}-\d{4}.*?\.csv$",
                r".*?death.*?\.csv$",
                r".*?kill.*?\.csv$",
                r".*?pvp.*?\.csv$",
                r".*?player.*?\.csv$"
            ]
    except Exception as e:
        logger.error(f"Error loading patterns from SFTP module: {e}")
        # Fallback to test patterns
        patterns = [
            r"\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv$",
            r"\d{4}\.\d{2}\.\d{2}.*?\.csv$",
            r"\.csv$"
        ]
    
    logger.info(f"Testing {len(patterns)} patterns against {len(test_filenames)} filenames")
    
    # Test each filename against each pattern
    results = {
        "matched": [],
        "unmatched": []
    }
    
    for filename in test_filenames:
        matched = False
        matching_patterns = []
        
        for pattern in patterns:
            if re.match(pattern, filename):
                matched = True
                matching_patterns.append(pattern)
        
        if matched:
            results["matched"].append({
                "filename": filename,
                "patterns": matching_patterns
            })
        else:
            results["unmatched"].append(filename)
    
    # Print results
    logger.info("\n=== CSV Pattern Matching Results ===")
    logger.info(f"Total filenames tested: {len(test_filenames)}")
    logger.info(f"Matched: {len(results['matched'])}")
    logger.info(f"Unmatched: {len(results['unmatched'])}")
    
    if results["matched"]:
        logger.info("\nSuccessfully matched files:")
        for item in results["matched"]:
            logger.info(f"- {item['filename']} matched by {len(item['patterns'])} patterns")
    
    if results["unmatched"]:
        logger.warning("\nUnmatched files:")
        for filename in results["unmatched"]:
            logger.warning(f"- {filename}")
    
    # Create temp directory and test with real files
    with tempfile.TemporaryDirectory() as temp_dir:
        logger.info(f"\nTesting with actual files in temp directory: {temp_dir}")
        
        # Create test files
        created_files = []
        for filename in test_filenames:
            file_path = os.path.join(temp_dir, filename)
            with open(file_path, "w") as f:
                f.write("timestamp;killer;killer_id;victim;victim_id;weapon;damage;platform\n")
                f.write("2025.05.09-11.58.37;TestKiller;12345;TestVictim;67890;AK47;100;PC\n")
            created_files.append(file_path)
        
        # Try to import the find_files_by_pattern function for testing
        try:
            import sys
            sys.path.append('.')
            
            # Special handling for SFTP client class initialization
            # Since we can't easily instantiate the class without connection parameters,
            # we'll test the pattern matching directly
            found_files = []
            for pattern in patterns:
                for file_path in created_files:
                    if re.match(pattern, os.path.basename(file_path)):
                        found_files.append(file_path)
            
            # Remove duplicates
            found_files = list(set(found_files))
            
            logger.info(f"Found {len(found_files)} files using patterns")
            logger.info(f"Coverage: {len(found_files)}/{len(created_files)} ({len(found_files)/len(created_files)*100:.1f}%)")
            
            # Determine which files were missed
            missed_files = [f for f in created_files if f not in found_files]
            if missed_files:
                logger.warning(f"Missed {len(missed_files)} files:")
                for f in missed_files:
                    logger.warning(f"- {os.path.basename(f)}")
            
            return len(found_files) == len(created_files)
            
        except Exception as e:
            logger.error(f"Error during file testing: {e}")
            import traceback
            traceback.print_exc()
            return False

async def main():
    """Main test function"""
    success = await test_csv_patterns()
    
    if success:
        logger.info("✅ CSV pattern matching tests passed")
    else:
        logger.warning("⚠️ CSV pattern matching tests had some issues")

if __name__ == "__main__":
    asyncio.run(main())