#!/usr/bin/env python3
"""
Fix Historical Parser

This script completely replaces the historical parser with a direct approach.
It is meant to be called directly from Python code or the command line.
"""
import os
import sys
import io
import csv
import re
import logging
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('historical_parse.log')
    ]
)

logger = logging.getLogger('historical_parser')

def direct_parse_csv(file_path: str, server_id: str) -> List[Dict[str, Any]]:
    """
    Parse a CSV file directly without complex infrastructure
    
    Args:
        file_path: Path to CSV file
        server_id: Server ID to include in events
        
    Returns:
        List of parsed event dictionaries
    """
    logger.info(f"Direct parsing file: {file_path}")
    
    try:
        # Read the file as binary
        with open(file_path, 'rb') as f:
            content = f.read()
            
        # Basic validation
        if not content or len(content) == 0:
            logger.error(f"Empty file: {file_path}")
            return []
            
        # Try to convert to string
        try:
            content_str = content.decode('utf-8', errors='replace')
        except Exception:
            try:
                content_str = content.decode('latin-1')
            except Exception as e:
                logger.error(f"Failed to decode file content: {e}")
                return []
                
        # Detect delimiter
        semicolons = content_str.count(';')
        commas = content_str.count(',')
        
        delimiter = ';'  # Game logs default
        if commas > semicolons*2:
            delimiter = ','
            
        # Parse with CSV reader
        events = []
        string_io = io.StringIO(content_str)
        csv_reader = csv.reader(string_io, delimiter=delimiter)
        
        # Process rows
        for row in csv_reader:
            # Skip empty rows
            if not row or len(row) < 3:
                continue
                
            # Create event dictionary
            event = {}
            
            # Standard format: timestamp, killer_name, killer_id, victim_name, victim_id, weapon, distance
            if len(row) >= 7:
                event['timestamp'] = row[0] if len(row) > 0 else ""
                event['killer_name'] = row[1] if len(row) > 1 else ""
                event['killer_id'] = row[2] if len(row) > 2 else ""
                event['victim_name'] = row[3] if len(row) > 3 else ""
                event['victim_id'] = row[4] if len(row) > 4 else ""
                event['weapon'] = row[5] if len(row) > 5 else ""
                event['distance'] = row[6] if len(row) > 6 else "0"
                
                # Optional platform field
                if len(row) > 7:
                    event['platform'] = row[7]
            else:
                # Handle shorter rows
                event['timestamp'] = row[0] if len(row) > 0 else datetime.now().strftime("%Y.%m.%d-%H.%M.%S")
                
                if len(row) == 3:  # Likely killer, victim, weapon
                    event['killer_name'] = row[0]
                    event['victim_name'] = row[1]
                    event['weapon'] = row[2]
                elif len(row) >= 4:  # Likely timestamp, killer, victim, weapon
                    event['killer_name'] = row[1]
                    event['victim_name'] = row[2]
                    event['weapon'] = row[3]
                    
            # Add server ID
            event['server_id'] = server_id
                    
            # Convert distance to float if possible
            try:
                event['distance'] = float(event.get('distance', 0))
            except (ValueError, TypeError):
                event['distance'] = 0.0
                
            # Try to parse timestamp
            try:
                ts_str = event.get('timestamp', '')
                formats = [
                    '%Y.%m.%d-%H.%M.%S',
                    '%Y.%m.%d-%H:%M:%S',
                    '%Y.%m.%d %H.%M.%S',
                    '%Y.%m.%d %H:%M:%S',
                    '%Y-%m-%d-%H.%M.%S',
                    '%Y-%m-%d-%H:%M:%S',
                    '%Y-%m-%d %H.%M.%S',
                    '%Y-%m-%d %H:%M:%S',
                ]
                
                parsed = False
                for fmt in formats:
                    try:
                        dt = datetime.strptime(ts_str, fmt)
                        event['timestamp'] = dt
                        parsed = True
                        break
                    except ValueError:
                        continue
                        
                if not parsed:
                    event['_timestamp_unparsed'] = True
            except Exception as e:
                logger.warning(f"Could not parse timestamp: {e}")
                
            # Add to events list
            events.append(event)
            
        logger.info(f"Successfully parsed {len(events)} events from {file_path}")
        return events
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

async def insert_events_to_db(db, events: List[Dict[str, Any]]) -> int:
    """
    Insert events directly into the database
    
    Args:
        db: Database connection
        events: List of events to insert
        
    Returns:
        Number of events successfully inserted
    """
    if not events:
        logger.warning("No events to insert")
        return 0
        
    # Initialize counters
    total_inserted = 0
    
    try:
        # Prepare kill and suicide events
        kill_events = []
        suicide_events = []
        
        # First categorize events
        for event in events:
            # Minimum required fields
            if not all(k in event for k in ['killer_name', 'victim_name']):
                continue
                
            # Check if this is a suicide
            is_suicide = event.get('killer_name') == event.get('victim_name')
            
            if is_suicide:
                suicide_events.append(event)
            else:
                kill_events.append(event)
                
        # Process kill events
        if kill_events:
            kill_docs = []
            
            for event in kill_events:
                # Required fields
                if not event.get('killer_id') or not event.get('victim_id'):
                    continue
                    
                # Create document
                kill_doc = {
                    "server_id": event.get('server_id'),
                    "killer_id": event.get('killer_id'),
                    "killer_name": event.get('killer_name', 'Unknown'),
                    "victim_id": event.get('victim_id'),
                    "victim_name": event.get('victim_name', 'Unknown'),
                    "weapon": event.get('weapon', 'Unknown'),
                    "distance": event.get('distance', 0),
                    "timestamp": event.get('timestamp', datetime.now()),
                    "is_suicide": False,
                    "event_type": "kill"
                }
                
                kill_docs.append(kill_doc)
                
            # Bulk insert
            if kill_docs:
                try:
                    # Insert with error handling
                    result = await db.kills.insert_many(kill_docs, ordered=False)
                    total_inserted += len(result.inserted_ids)
                    logger.info(f"Inserted {len(result.inserted_ids)} kill events")
                except Exception as e:
                    logger.error(f"Error inserting kill events: {str(e)[:100]}")
                    
        # Process suicide events
        if suicide_events:
            suicide_docs = []
            
            for event in suicide_events:
                # Required fields
                if not event.get('victim_id'):
                    continue
                    
                # Create document
                suicide_doc = {
                    "server_id": event.get('server_id'),
                    "killer_id": event.get('victim_id'),
                    "killer_name": event.get('victim_name', 'Unknown'),
                    "victim_id": event.get('victim_id'),
                    "victim_name": event.get('victim_name', 'Unknown'),
                    "weapon": event.get('weapon', 'Unknown'),
                    "distance": event.get('distance', 0),
                    "timestamp": event.get('timestamp', datetime.now()),
                    "is_suicide": True,
                    "event_type": "suicide"
                }
                
                suicide_docs.append(suicide_doc)
                
            # Bulk insert
            if suicide_docs:
                try:
                    # Insert with error handling
                    result = await db.kills.insert_many(suicide_docs, ordered=False)
                    total_inserted += len(result.inserted_ids)
                    logger.info(f"Inserted {len(result.inserted_ids)} suicide events")
                except Exception as e:
                    logger.error(f"Error inserting suicide events: {str(e)[:100]}")
                    
        return total_inserted
    except Exception as e:
        logger.error(f"Error during database insertion: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0

async def standalone_historical_parser(server_id: str, days: int = 30, db=None):
    """
    Run historical parsing as a standalone process
    
    Args:
        server_id: Server ID
        days: Days to look back
        db: Database connection
        
    Returns:
        Tuple of (files_processed, events_inserted)
    """
    logger.info(f"Starting standalone historical parser for server {server_id}, looking back {days} days")
    
    # Safety check
    if not server_id:
        logger.error("No server ID provided")
        return 0, 0
        
    # Check for test assets directory
    assets_dir = './attached_assets'
    if not os.path.exists(assets_dir):
        logger.error(f"Assets directory not found: {assets_dir}")
        return 0, 0
        
    # Calculate cutoff date
    cutoff_date = datetime.now() - timedelta(days=days)
    logger.info(f"Reference date: {cutoff_date} (but including ALL files)")
    
    # Find ALL CSV files regardless of date
    csv_files = []
    for filename in os.listdir(assets_dir):
        if filename.endswith('.csv'):
            # For reporting purposes, try to get the date, but include all files regardless
            date_match = re.search(r'(\d{4}\.\d{2}\.\d{2})', filename)
            if date_match:
                try:
                    file_date = datetime.strptime(date_match.group(1), '%Y.%m.%d')
                    if file_date < cutoff_date:
                        logger.info(f"Including older file: {filename} (date: {file_date})")
                    else:
                        logger.info(f"Including recent file: {filename} (date: {file_date})")
                except ValueError:
                    logger.info(f"Date parse failed but including file: {filename}")
            else:
                logger.info(f"No date pattern in filename, including: {filename}")
            
            # ALWAYS include all CSV files
            csv_files.append(os.path.join(assets_dir, filename))
                
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    # Process files
    files_processed = 0
    events_inserted = 0
    
    for file_path in csv_files:
        logger.info(f"Processing file: {file_path}")
        
        # Parse the file
        events = direct_parse_csv(file_path, server_id)
        
        if events:
            logger.info(f"Parsed {len(events)} events from {file_path}")
            
            # Insert to database if provided
            if db:
                inserted = await insert_events_to_db(db, events)
                events_inserted += inserted
                logger.info(f"Inserted {inserted} events from {file_path}")
            
            files_processed += 1
        else:
            logger.warning(f"No events parsed from {file_path}")
            
    logger.info(f"Historical parsing complete: {files_processed} files processed, {events_inserted} events inserted")
    return files_processed, events_inserted

# Can be imported and used directly from Python code or run as a script
if __name__ == "__main__":
    # This is only used for local testing
    # In production, call standalone_historical_parser() function directly
    try:
        # Sample server ID for testing
        test_server_id = "test_server_001"
        asyncio.run(standalone_historical_parser(test_server_id))
    except Exception as e:
        logger.error(f"Error running standalone parser: {e}")
        import traceback
        logger.error(traceback.format_exc())