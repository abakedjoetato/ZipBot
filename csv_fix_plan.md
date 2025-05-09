# CSV Parser Issue Analysis and Fix Plan

## Problem Identification

After thorough analysis of the codebase, I've identified several issues related to the CSV parsing system that cause files to be found but not properly processed:

1. **Delimiter Parameter Inconsistency**: The `CSV_Parser` class methods don't consistently accept and handle delimiter parameters passed from the CSV processor.

2. **CSV File Pattern Filtering**: The pattern matching used to find CSV files may be too restrictive, causing some valid files to be found but not selected for parsing.

3. **Date/Timestamp Parsing Issues**: There appears to be inconsistent handling of timestamp formats in the CSV data, particularly with the `YYYY.MM.DD-HH.MM.SS` format.

4. **Cache Interference**: The `last_processed` and `processed_files` caches might be preventing reprocessing of previously seen files, even when they weren't successfully parsed.

## Root Cause Analysis

The primary issue appears to be in the CSV parser's handling of delimiters. The code in `fix_csv_parser_delimiter.py` reveals that there's a mismatch where the processor tries to pass a delimiter parameter to the parser, but the parser isn't consistently accepting or using this parameter.

This causes CSV files to be found and downloaded, but then skipped during parsing because:
1. The wrong delimiter is used (expecting `,` when the file uses `;`)
2. The parser silently fails when encountering improperly formatted rows
3. No events are extracted, so the file is considered "processed" but with zero events

## Fix Implementation Plan

I'll implement a comprehensive fix that addresses all aspects of the issue:

1. **Fix Delimiter Parameter Handling**:
   - Update `_parse_csv_file` method to consistently accept and use a delimiter parameter 
   - Ensure `parse_csv_data` properly passes the delimiter parameter to `_parse_csv_file`
   - Add robust delimiter auto-detection when none is explicitly provided

2. **Improve CSV File Pattern Recognition**:
   - Enhance the pattern matching to recognize all valid CSV file formats
   - Implement fallback detection for various naming conventions
   - Add more diagnostic logging when files are found but not parsed

3. **Strengthen Timestamp Parsing**:
   - Ensure all date/time format variations are properly handled
   - Implement more robust error handling for malformed timestamps
   - Add explicit format detection and conversion

4. **Fix Cache Management**:
   - Reset appropriate cache entries when reprocessing is requested
   - Track parse success separately from download success
   - Implement a mechanism to force reprocessing of previously skipped files

5. **Add Enhanced Logging**:
   - Add detailed logging at key points in the file processing flow
   - Report both succeeded and failed parsing attempts clearly
   - Ensure file content samples are logged for diagnostic purposes

## Testing Plan

After implementing the fixes, I'll validate the solution using:

1. **Direct CSV Tests**:
   - Test with sample files from attached_assets
   - Test with files actually found on the SFTP server
   - Verify correct delimiter detection and usage

2. **Historical Parse Test**:
   - Trigger a historical parse to verify older files can be processed
   - Verify the timestamps are correctly parsed
   - Ensure all previously skipped files are properly processed

3. **Integration Test**:
   - Run the bot with the fixes applied
   - Verify events from CSV files are properly stored in the database
   - Confirm the file processing metrics match expectations

## Verification Approach

I'll use the existing diagnostic tools to verify the fix:
- `direct_csv_test.py` to test the parsing directly
- `verify_csv_fix.py` to validate the complete flow
- `csv_deep_diagnostic.py` to verify all components are working together

The success criteria will be:
1. All CSV files found are successfully parsed
2. All timestamps are correctly converted to datetime objects
3. All events from the CSV files are properly extracted and stored
4. No files are skipped inappropriately during processing