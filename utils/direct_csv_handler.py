"""
Direct CSV Handler

This module provides a completely separate, simplified CSV parsing implementation
that bypasses all the complex infrastructure of the main application.

It's designed to be used as a fallback when the main parsing logic fails.
"""
import os
import csv
import io
import logging
import traceback
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union, cast

# Set up logging
logger = logging.getLogger(__name__)

def direct_parse_csv_file(file_path: str, server_id: str) -> List[Dict[str, Any]]:
    """
    Direct, simplified CSV parsing implementation that bypasses all complex infrastructure.
    
    Args:
        file_path: Path to the CSV file
        server_id: Server ID to associate with the events
        
    Returns:
        List of parsed event dictionaries
    """
    logger.info(f"Direct parsing CSV file: {file_path}")
    
    try:
        # Read file as binary
        with open(file_path, 'rb') as f:
            content = f.read()
            
        if not content:
            logger.error(f"Empty file: {file_path}")
            return []
            
        # Try multiple encodings
        content_str = None
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                content_str = content.decode(encoding, errors='replace')
                break
            except Exception:
                continue
                
        if not content_str:
            logger.error(f"Failed to decode file content: {file_path}")
            return []
            
        # Detect delimiter (semicolons or commas)
        semicolons = content_str.count(';')
        commas = content_str.count(',')
        
        delimiter = ';'  # Default for most game logs
        if commas > semicolons * 2:
            delimiter = ','
            
        logger.info(f"Using delimiter '{delimiter}' for {file_path}")
        
        # Create CSV reader
        csv_reader = csv.reader(io.StringIO(content_str), delimiter=delimiter)
        
        # Parse events
        events = []
        row_count = 0
        
        for row in csv_reader:
            row_count += 1
            
            # Skip empty rows or those without enough fields
            if not row or len(row) < 5:
                continue
                
            # Skip header rows
            if any(keyword in row[0].lower() for keyword in ['time', 'date', 'timestamp']):
                continue
                
            # Extract data from row
            try:
                event = {
                    'timestamp': row[0] if len(row) > 0 else "",
                    'killer_name': row[1] if len(row) > 1 else "",
                    'killer_id': row[2] if len(row) > 2 else "",
                    'victim_name': row[3] if len(row) > 3 else "",
                    'victim_id': row[4] if len(row) > 4 else "",
                    'weapon': row[5] if len(row) > 5 else "",
                    'distance': float(row[6]) if len(row) > 6 and row[6].strip() else 0.0,
                    'server_id': server_id,
                    'event_type': 'kill'
                }
            except Exception as e:
                logger.error(f"Error processing row: {e}")
                continue
            
            # Check for suicide (killer == victim)
            if event['killer_name'] == event['victim_name'] or event['killer_id'] == event['victim_id']:
                event['event_type'] = 'suicide'
                event['is_suicide'] = True
            else:
                event['is_suicide'] = False
                
            # Parse timestamp
            try:
                ts_str = event['timestamp']
                
                # Try various timestamp formats
                timestamp_formats = [
                    '%Y.%m.%d-%H.%M.%S',
                    '%Y.%m.%d-%H:%M:%S',
                    '%Y.%m.%d %H.%M.%S',
                    '%Y.%m.%d %H:%M:%S',
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d %H.%M.%S',
                    '%m/%d/%Y %H:%M:%S',
                    '%d/%m/%Y %H:%M:%S'
                ]
                
                for fmt in timestamp_formats:
                    try:
                        dt = datetime.strptime(ts_str, fmt)
                        event['timestamp'] = dt
                        break
                    except ValueError:
                        continue
                        
                # If still a string, use current time
                if isinstance(event['timestamp'], str):
                    event['timestamp'] = datetime.now()
                    
            except Exception as e:
                logger.error(f"Error parsing timestamp: {e}")
                event['timestamp'] = datetime.now()
                
            # Add to events list
            events.append(event)
            
        logger.info(f"Directly parsed {len(events)} events from {row_count} rows in {file_path}")
        return events
        
    except Exception as e:
        logger.error(f"Error in direct CSV parsing of {file_path}: {e}")
        logger.error(traceback.format_exc())
        return []

async def direct_import_events(db, events: List[Dict[str, Any]]) -> int:
    """
    Import events directly into the database.
    
    Args:
        db: Database connection
        events: List of events to import
        
    Returns:
        Number of events imported
    """
    if not events:
        return 0
        
    logger.info(f"Directly importing {len(events)} events")
    
    try:
        result = await db.kills.insert_many(events)
        imported = len(result.inserted_ids)
        logger.info(f"Successfully imported {imported} events directly")
        return imported
    except Exception as e:
        logger.error(f"Error importing events: {e}")
        logger.error(traceback.format_exc())
        return 0

async def process_directory(db, server_id: str, days: int = 30) -> Tuple[int, int]:
    """
    Process all CSV files in the attached_assets directory.
    
    Args:
        db: Database connection
        server_id: Server ID to use for the events
        days: Number of days to look back
        
    Returns:
        Tuple of (files_processed, events_imported)
    """
    logger.info(f"Processing CSV files from SFTP for server {server_id}, looking back {days} days")
    
    if not server_id:
        logger.error("No server ID provided")
        return 0, 0
        
    # Find CSV files newer than cutoff date
    csv_files = []
    cutoff_date = datetime.now() - timedelta(days=days)
    
    for filename in os.listdir(ASSETS_DIR):
        if not filename.endswith('.csv'):
            continue
            
        # For logging, try to extract date from filename
        date_match = re.search(r'(\d{4})\.(\d{2})\.(\d{2})', filename)
        
        if date_match:
            year, month, day = map(int, date_match.groups())
            try:
                file_date = datetime(year, month, day)
                logger.info(f"Including file {filename} (date: {file_date.strftime('%Y-%m-%d')})")
            except ValueError:
                # If date parsing fails, still include file
                logger.info(f"Including file {filename} (date parsing failed)")
        else:
            logger.info(f"Including file {filename} (no date in filename)")
                
        # Always include all CSV files
        csv_files.append(os.path.join(ASSETS_DIR, filename))
    
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    # Process each file
    files_processed = 0
    events_imported = 0
    
    for file_path in csv_files:
        # Parse events
        events = direct_parse_csv_file(file_path, server_id)
        
        if events:
            # Import events
            imported = await direct_import_events(db, events)
            if imported > 0:
                files_processed += 1
                events_imported += imported
                logger.info(f"Processed file {file_path}: imported {imported} events")
            else:
                logger.warning(f"Failed to import events from {file_path}")
        else:
            logger.warning(f"No events parsed from {file_path}")
    
    logger.info(f"Direct processing complete: processed {files_processed} files, imported {events_imported} events")
    return files_processed, events_imported

async def update_player_stats(db, server_id: str) -> int:
    """
    Update player statistics based on kill events.
    
    Args:
        db: Database connection
        server_id: Server ID to update stats for
        
    Returns:
        Number of players updated
    """
    logger.info(f"Updating player statistics for server {server_id}")
    
    try:
        # Get all kill events for this server
        kill_cursor = db.kills.find({"server_id": server_id})
        
        # Group by player
        player_stats = {}
        
        async for event in kill_cursor:
            killer_id = event.get('killer_id')
            killer_name = event.get('killer_name')
            victim_id = event.get('victim_id')
            victim_name = event.get('victim_name')
            is_suicide = event.get('is_suicide', False)
            
            if not killer_id or not victim_id:
                continue
                
            # Update killer stats
            if killer_id not in player_stats:
                player_stats[killer_id] = {
                    'player_id': killer_id,
                    'name': killer_name,
                    'server_id': server_id,
                    'kills': 0,
                    'deaths': 0,
                    'suicides': 0
                }
                
            # Update victim stats
            if victim_id not in player_stats:
                player_stats[victim_id] = {
                    'player_id': victim_id,
                    'name': victim_name,
                    'server_id': server_id,
                    'kills': 0,
                    'deaths': 0,
                    'suicides': 0
                }
                
            if is_suicide:
                player_stats[killer_id]['suicides'] += 1
                player_stats[killer_id]['deaths'] += 1
            else:
                player_stats[killer_id]['kills'] += 1
                player_stats[victim_id]['deaths'] += 1
        
        # Update player documents
        updated_count = 0
        
        for player_id, stats in player_stats.items():
            # Try to find existing player
            player = await db.players.find_one({
                'server_id': server_id,
                'player_id': player_id
            })
            
            if player:
                # Update existing player
                result = await db.players.update_one(
                    {'_id': player['_id']},
                    {
                        '$set': {
                            'name': stats['name'],
                            'kills': stats['kills'],
                            'deaths': stats['deaths'],
                            'suicides': stats['suicides'],
                            'updated_at': datetime.now()
                        }
                    }
                )
                
                if result.modified_count > 0:
                    updated_count += 1
            else:
                # Create new player
                stats['created_at'] = datetime.now()
                stats['updated_at'] = datetime.now()
                
                result = await db.players.insert_one(stats)
                if result.inserted_id:
                    updated_count += 1
        
        logger.info(f"Updated {updated_count} players for server {server_id}")
        return updated_count
        
    except Exception as e:
        logger.error(f"Error updating player statistics: {e}")
        logger.error(traceback.format_exc())
        return 0

async def update_rivalries(db, server_id: str) -> int:
    """
    Update rivalries based on kill events.
    
    Args:
        db: Database connection
        server_id: Server ID to update rivalries for
        
    Returns:
        Number of rivalries updated
    """
    logger.info(f"Updating rivalries for server {server_id}")
    
    try:
        # Get non-suicide kill events
        kill_cursor = db.kills.find({
            "server_id": server_id,
            "is_suicide": {"$ne": True}
        })
        
        # Count kills between players
        rivalry_counts = {}
        
        async for event in kill_cursor:
            killer_id = event.get('killer_id')
            killer_name = event.get('killer_name')
            victim_id = event.get('victim_id')
            victim_name = event.get('victim_name')
            
            if not killer_id or not victim_id:
                continue
                
            rivalry_key = f"{killer_id}:{victim_id}"
            
            if rivalry_key not in rivalry_counts:
                rivalry_counts[rivalry_key] = {
                    'killer_id': killer_id,
                    'killer_name': killer_name,
                    'victim_id': victim_id,
                    'victim_name': victim_name,
                    'server_id': server_id,
                    'kills': 0
                }
                
            rivalry_counts[rivalry_key]['kills'] += 1
        
        # Update rivalry documents
        updated_count = 0
        
        for key, data in rivalry_counts.items():
            # Skip if no kills
            if data['kills'] == 0:
                continue
                
            # Try to find existing rivalry
            rivalry = await db.rivalries.find_one({
                'server_id': server_id,
                'killer_id': data['killer_id'],
                'victim_id': data['victim_id']
            })
            
            if rivalry:
                # Update existing rivalry
                result = await db.rivalries.update_one(
                    {'_id': rivalry['_id']},
                    {
                        '$set': {
                            'killer_name': data['killer_name'],
                            'victim_name': data['victim_name'],
                            'kills': data['kills'],
                            'updated_at': datetime.now()
                        }
                    }
                )
                
                if result.modified_count > 0:
                    updated_count += 1
            else:
                # Create new rivalry
                data['created_at'] = datetime.now()
                data['updated_at'] = datetime.now()
                
                result = await db.rivalries.insert_one(data)
                if result.inserted_id:
                    updated_count += 1
        
        logger.info(f"Updated {updated_count} rivalries for server {server_id}")
        return updated_count
        
    except Exception as e:
        logger.error(f"Error updating rivalries: {e}")
        logger.error(traceback.format_exc())
        return 0