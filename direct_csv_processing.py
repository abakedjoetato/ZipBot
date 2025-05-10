#!/usr/bin/env python3
"""
Direct CSV Processing

This is a standalone script to process CSV files directly from a directory
and insert them into MongoDB, completely bypassing all bot infrastructure.

Usage:
    python direct_csv_processing.py [--server_id SERVER_ID] [--days DAYS]
"""
import os
import sys
import io
import csv
import re
import logging
import traceback
import argparse
import asyncio
import motor.motor_asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('direct_csv.log')
    ]
)

logger = logging.getLogger('direct_csv')

# Constants
ASSETS_DIR = './attached_assets'
MONGO_URI = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/pvpbot')
DB_NAME = 'pvpbot'
COLLECTION_KILLS = 'kills'
COLLECTION_PLAYERS = 'players'
COLLECTION_RIVALRIES = 'rivalries'
BATCH_SIZE = 100

def parse_csv_file(file_path: str, server_id: str) -> List[Dict[str, Any]]:
    """
    Parse a CSV file and extract kill events.
    
    Args:
        file_path: Path to the CSV file
        server_id: Server ID to associate with events
        
    Returns:
        List of parsed kill event dictionaries
    """
    logger.info(f"Parsing CSV file: {file_path}")
    
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
            
        if not content:
            logger.warning(f"Empty file: {file_path}")
            return []
            
        # Try to convert content to string
        try:
            content_str = content.decode('utf-8', errors='replace')
        except Exception:
            try:
                content_str = content.decode('latin-1')
            except Exception as e:
                logger.error(f"Failed to decode content: {e}")
                return []
                
        # Detect delimiter (prioritize semicolons)
        semicolons = content_str.count(';')
        commas = content_str.count(',')
        
        delimiter = ';'  # Default for game logs
        if commas > semicolons * 2:
            delimiter = ','
            
        logger.info(f"Using delimiter '{delimiter}' for {file_path}")
        
        # Create CSV reader
        string_io = io.StringIO(content_str)
        csv_reader = csv.reader(string_io, delimiter=delimiter)
        
        # Process rows
        events = []
        row_count = 0
        
        for row in csv_reader:
            row_count += 1
            
            # Skip empty rows or rows without enough fields
            if not row or len(row) < 6:  # Minimum: timestamp, killer, killer_id, victim, victim_id, weapon
                continue
                
            # Create event dictionary based on field count
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
            
            # Check if this appears to be a header row
            if 'time' in event['timestamp'].lower() or 'date' in event['timestamp'].lower():
                continue
                
            # Check if this is a suicide (same killer and victim)
            if event['killer_name'] == event['victim_name'] or event['killer_id'] == event['victim_id']:
                event['event_type'] = 'suicide'
                event['is_suicide'] = True
            else:
                event['is_suicide'] = False
                
            # Try to parse timestamp
            try:
                ts_str = event['timestamp']
                
                for fmt in [
                    '%Y.%m.%d-%H.%M.%S',
                    '%Y.%m.%d-%H:%M:%S',
                    '%Y.%m.%d %H.%M.%S',
                    '%Y.%m.%d %H:%M:%S'
                ]:
                    try:
                        dt = datetime.strptime(ts_str, fmt)
                        event['timestamp'] = dt
                        break
                    except ValueError:
                        continue
            except Exception:
                # Just use current time if we can't parse
                event['timestamp'] = datetime.now()
                
            # Add event to list
            events.append(event)
            
        logger.info(f"Parsed {len(events)} events from {row_count} rows in {file_path}")
        return events
        
    except Exception as e:
        logger.error(f"Error parsing file {file_path}: {e}")
        logger.error(traceback.format_exc())
        return []

async def connect_to_mongodb() -> Tuple[Any, Any]:
    """
    Connect to MongoDB and return client and database objects.
    
    Returns:
        Tuple of (client, database)
    """
    try:
        logger.info(f"Connecting to MongoDB: {MONGO_URI}")
        client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        db = client[DB_NAME]
        logger.info(f"Connected to database: {DB_NAME}")
        return client, db
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        raise

async def insert_events(db, events: List[Dict[str, Any]]) -> int:
    """
    Insert events into MongoDB.
    
    Args:
        db: MongoDB database connection
        events: List of events to insert
        
    Returns:
        Number of events inserted
    """
    logger.info(f"Inserting {len(events)} events into database")
    
    # Group events by type
    kill_events = [e for e in events if not e.get('is_suicide', False)]
    suicide_events = [e for e in events if e.get('is_suicide', False)]
    
    logger.info(f"Grouped into {len(kill_events)} kills and {len(suicide_events)} suicides")
    
    # Insert in batches
    inserted_count = 0
    
    try:
        # Process kill events in batches
        for i in range(0, len(kill_events), BATCH_SIZE):
            batch = kill_events[i:i+BATCH_SIZE]
            if batch:
                try:
                    result = await db[COLLECTION_KILLS].insert_many(batch, ordered=False)
                    inserted_count += len(result.inserted_ids)
                    logger.info(f"Inserted batch of {len(result.inserted_ids)} kill events")
                except Exception as e:
                    logger.error(f"Error inserting kill batch: {e}")
        
        # Process suicide events in batches
        for i in range(0, len(suicide_events), BATCH_SIZE):
            batch = suicide_events[i:i+BATCH_SIZE]
            if batch:
                try:
                    result = await db[COLLECTION_KILLS].insert_many(batch, ordered=False)
                    inserted_count += len(result.inserted_ids)
                    logger.info(f"Inserted batch of {len(result.inserted_ids)} suicide events")
                except Exception as e:
                    logger.error(f"Error inserting suicide batch: {e}")
                    
        logger.info(f"Total inserted: {inserted_count} events")
        return inserted_count
    except Exception as e:
        logger.error(f"Error during batch insertion: {e}")
        logger.error(traceback.format_exc())
        return 0

async def update_player_stats(db, events: List[Dict[str, Any]]) -> int:
    """
    Update player statistics based on events.
    
    Args:
        db: MongoDB database connection
        events: List of events
        
    Returns:
        Number of players updated
    """
    logger.info(f"Updating player stats for {len(events)} events")
    
    # Group events by player
    killer_stats = {}
    victim_stats = {}
    
    for event in events:
        server_id = event.get('server_id')
        killer_id = event.get('killer_id')
        victim_id = event.get('victim_id')
        is_suicide = event.get('is_suicide', False)
        
        if not server_id or not killer_id or not victim_id:
            continue
        
        if is_suicide:
            # For suicides, only update the player's suicide count
            player_key = f"{server_id}:{killer_id}"
            
            if player_key not in victim_stats:
                victim_stats[player_key] = {
                    'server_id': server_id,
                    'player_id': killer_id,
                    'suicides': 0,
                    'deaths': 0,
                    'kills': 0,
                    'name': event.get('killer_name', 'Unknown')
                }
            
            victim_stats[player_key]['suicides'] += 1
            victim_stats[player_key]['deaths'] += 1
        else:
            # For kills, update both the killer and the victim
            killer_key = f"{server_id}:{killer_id}"
            victim_key = f"{server_id}:{victim_id}"
            
            # Update killer stats
            if killer_key not in killer_stats:
                killer_stats[killer_key] = {
                    'server_id': server_id,
                    'player_id': killer_id,
                    'kills': 0,
                    'deaths': 0,
                    'suicides': 0,
                    'name': event.get('killer_name', 'Unknown')
                }
            
            killer_stats[killer_key]['kills'] += 1
            
            # Update victim stats
            if victim_key not in victim_stats:
                victim_stats[victim_key] = {
                    'server_id': server_id,
                    'player_id': victim_id,
                    'deaths': 0,
                    'kills': 0,
                    'suicides': 0,
                    'name': event.get('victim_name', 'Unknown')
                }
            
            victim_stats[victim_key]['deaths'] += 1
    
    # Combine stats
    all_player_stats = {}
    all_player_stats.update(killer_stats)
    
    # Merge victim stats (some players might be both killers and victims)
    for key, stats in victim_stats.items():
        if key in all_player_stats:
            all_player_stats[key]['deaths'] += stats['deaths']
            all_player_stats[key]['suicides'] += stats['suicides']
        else:
            all_player_stats[key] = stats
    
    # Update player documents
    updated_count = 0
    for key, stats in all_player_stats.items():
        try:
            # Try to find existing player
            player = await db[COLLECTION_PLAYERS].find_one({
                'server_id': stats['server_id'],
                'player_id': stats['player_id']
            })
            
            if player:
                # Update existing player
                result = await db[COLLECTION_PLAYERS].update_one(
                    {'_id': player['_id']},
                    {
                        '$set': {
                            'name': stats['name'],
                            'updated_at': datetime.now()
                        },
                        '$inc': {
                            'kills': stats['kills'],
                            'deaths': stats['deaths'],
                            'suicides': stats['suicides']
                        }
                    }
                )
                
                if result.modified_count > 0:
                    updated_count += 1
            else:
                # Create new player
                new_player = {
                    'server_id': stats['server_id'],
                    'player_id': stats['player_id'],
                    'name': stats['name'],
                    'kills': stats['kills'],
                    'deaths': stats['deaths'],
                    'suicides': stats['suicides'],
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                
                result = await db[COLLECTION_PLAYERS].insert_one(new_player)
                if result.inserted_id:
                    updated_count += 1
                    
        except Exception as e:
            logger.error(f"Error updating player {key}: {e}")
    
    logger.info(f"Updated {updated_count} players")
    return updated_count

async def update_rivalries(db, events: List[Dict[str, Any]]) -> int:
    """
    Update rivalries based on kill events.
    
    Args:
        db: MongoDB database connection
        events: List of kill events
        
    Returns:
        Number of rivalries updated
    """
    logger.info(f"Updating rivalries for {len(events)} events")
    
    # Count kills between players
    rivalry_counts = {}
    
    for event in events:
        if event.get('is_suicide', False):
            continue  # Skip suicides
            
        server_id = event.get('server_id')
        killer_id = event.get('killer_id')
        victim_id = event.get('victim_id')
        
        if not server_id or not killer_id or not victim_id:
            continue
            
        rivalry_key = f"{server_id}:{killer_id}:{victim_id}"
        
        if rivalry_key not in rivalry_counts:
            rivalry_counts[rivalry_key] = {
                'server_id': server_id,
                'killer_id': killer_id,
                'killer_name': event.get('killer_name', 'Unknown'),
                'victim_id': victim_id,
                'victim_name': event.get('victim_name', 'Unknown'),
                'kills': 0
            }
            
        rivalry_counts[rivalry_key]['kills'] += 1
    
    # Update rivalry documents
    updated_count = 0
    for key, data in rivalry_counts.items():
        try:
            # Try to find existing rivalry
            rivalry = await db[COLLECTION_RIVALRIES].find_one({
                'server_id': data['server_id'],
                'killer_id': data['killer_id'],
                'victim_id': data['victim_id']
            })
            
            if rivalry:
                # Update existing rivalry
                result = await db[COLLECTION_RIVALRIES].update_one(
                    {'_id': rivalry['_id']},
                    {
                        '$set': {
                            'killer_name': data['killer_name'],
                            'victim_name': data['victim_name'],
                            'updated_at': datetime.now()
                        },
                        '$inc': {
                            'kills': data['kills']
                        }
                    }
                )
                
                if result.modified_count > 0:
                    updated_count += 1
            else:
                # Create new rivalry
                new_rivalry = {
                    'server_id': data['server_id'],
                    'killer_id': data['killer_id'],
                    'killer_name': data['killer_name'],
                    'victim_id': data['victim_id'],
                    'victim_name': data['victim_name'],
                    'kills': data['kills'],
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                
                result = await db[COLLECTION_RIVALRIES].insert_one(new_rivalry)
                if result.inserted_id:
                    updated_count += 1
                    
        except Exception as e:
            logger.error(f"Error updating rivalry {key}: {e}")
    
    logger.info(f"Updated {updated_count} rivalries")
    return updated_count

async def process_directory(db, server_id: str, days: int = 30) -> Tuple[int, int]:
    """
    Process all CSV files in the assets directory.
    
    Args:
        db: MongoDB database connection
        server_id: Server ID for events
        days: Number of days to look back
        
    Returns:
        Tuple of (files_processed, events_inserted)
    """
    logger.info(f"Processing directory {ASSETS_DIR} for server {server_id}, looking back {days} days")
    
    if not os.path.exists(ASSETS_DIR):
        logger.error(f"Directory {ASSETS_DIR} does not exist")
        return 0, 0
        
    # Find CSV files
    csv_files = []
    cutoff_date = datetime.now() - timedelta(days=days)
    
    for filename in os.listdir(ASSETS_DIR):
        if not filename.endswith('.csv'):
            continue
            
        # Try to extract date from filename for logging purposes only
        date_match = re.search(r'(\d{4})\.(\d{2})\.(\d{2})', filename)
        
        if date_match:
            year, month, day = map(int, date_match.groups())
            try:
                file_date = datetime(year, month, day)
                
                # Log but don't filter by date - we want ALL csv files
                if file_date < cutoff_date:
                    logger.info(f"Including older file {filename} (date: {file_date.strftime('%Y-%m-%d')})")
                else:
                    logger.info(f"File {filename} is within the requested {days} day window")
                    
            except ValueError:
                # If date parsing fails, include file
                logger.info(f"Couldn't parse date from filename {filename}, including anyway")
                pass
        else:
            logger.info(f"No date pattern in filename {filename}, including anyway")
                
        # Include ALL csv files
        csv_files.append(os.path.join(ASSETS_DIR, filename))
    
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    # Process each file
    files_processed = 0
    total_events = 0
    all_events = []
    
    for file_path in csv_files:
        logger.info(f"Processing file {file_path}")
        
        # Parse events
        events = parse_csv_file(file_path, server_id)
        
        if events:
            all_events.extend(events)
            files_processed += 1
            logger.info(f"Added {len(events)} events from {file_path}")
        else:
            logger.warning(f"No events found in {file_path}")
    
    # Insert all events at once
    if all_events:
        # Insert events
        inserted_count = await insert_events(db, all_events)
        
        # Update player stats
        updated_players = await update_player_stats(db, all_events)
        
        # Update rivalries
        updated_rivalries = await update_rivalries(db, all_events)
        
        logger.info(f"Processing complete: {files_processed} files, {inserted_count} events inserted")
        logger.info(f"Updated {updated_players} players and {updated_rivalries} rivalries")
        
        return files_processed, inserted_count
    else:
        logger.warning("No events found in any files")
        return 0, 0

async def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Process CSV files directly into MongoDB')
    parser.add_argument('--server_id', type=str, default='test_server', help='Server ID for events')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back')
    
    args = parser.parse_args()
    
    try:
        # Connect to MongoDB
        client, db = await connect_to_mongodb()
        
        try:
            # Process directory
            files_processed, events_inserted = await process_directory(
                db=db,
                server_id=args.server_id,
                days=args.days
            )
            
            logger.info(f"Script completed successfully: {files_processed} files processed, {events_inserted} events inserted")
            
        finally:
            # Close MongoDB connection
            client.close()
            logger.info("MongoDB connection closed")
            
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())