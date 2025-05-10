#!/usr/bin/env python3
"""
Direct SFTP Test Script

This standalone script tests the SFTP connection and file listing/reading
completely independently from the bot infrastructure.
"""
import os
import sys
import re
import io
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('sftp_test.log')
    ]
)

logger = logging.getLogger('direct_sftp_test')

# Test SFTP configuration
TEST_SERVER_CONFIG = {
    "sftp_host": os.environ.get("TEST_SFTP_HOST", "127.0.0.1"),
    "sftp_port": int(os.environ.get("TEST_SFTP_PORT", "22")),
    "sftp_username": os.environ.get("TEST_SFTP_USERNAME", "test"),
    "sftp_password": os.environ.get("TEST_SFTP_PASSWORD", "password"),
    "deathlog_path": os.environ.get("TEST_DEATHLOG_PATH", "/deathlogs"),
    "server_id": "test_server",
    "server_uuid": "00000000-0000-0000-0000-000000000000",
    "server_name": "Test Server",
    "guild_id": None
}

async def direct_sftp_test(config: Dict[str, Any]):
    """
    Test SFTP connection and file operations directly
    
    Args:
        config: SFTP server configuration
    """
    logger.info(f"Testing SFTP connection to {config['sftp_host']}:{config['sftp_port']}")
    
    try:
        # Import here to avoid dependency issues
        import asyncssh
        logger.info("asyncssh module successfully imported")
    except ImportError:
        logger.error("asyncssh module not installed. Install with: pip install asyncssh")
        return False
    
    try:
        # Test connection with timeouts
        logger.info("Connecting to SFTP server...")
        
        # Clone config to avoid modifying the original
        conn_config = config.copy()
        
        # Get connection details
        host = conn_config.get('sftp_host')
        port = conn_config.get('sftp_port', 22)
        username = conn_config.get('sftp_username')
        password = conn_config.get('sftp_password')
        path = conn_config.get('deathlog_path', '/')
        
        logger.debug(f"Connection details - Host: {host}, Port: {port}, User: {username}, Path: {path}")
        
        # Verify required parameters
        if not all([host, username, password]):
            logger.error("Missing required SFTP parameters - need host, username, and password")
            return False
        
        logger.info(f"Attempting connection to {host}:{port} as {username}")
        
        # Add explicit timeouts
        conn = None
        try:
            # Connect with timeout
            conn = await asyncio.wait_for(
                asyncssh.connect(
                    host=host,
                    port=port,
                    username=username,
                    password=password,
                    known_hosts=None  # Disable host key checking for test
                ),
                timeout=10.0  # 10 second connection timeout
            )
            logger.info("Successfully connected to SFTP server")
            
            # Open SFTP client session
            async with conn.start_sftp_client() as sftp:
                logger.info("SFTP session established successfully")
                
                # List files in deathlog directory
                logger.info(f"Listing files in {path}")
                try:
                    files = await asyncio.wait_for(
                        sftp.listdir(path),
                        timeout=5.0  # 5 second timeout for directory listing
                    )
                    logger.info(f"Found {len(files)} files/directories in {path}")
                    
                    # Log the first 10 files
                    for i, filename in enumerate(files[:10]):
                        logger.info(f"File {i+1}: {filename}")
                    
                    # Count CSV files
                    csv_files = [f for f in files if f.lower().endswith('.csv')]
                    logger.info(f"Found {len(csv_files)} CSV files in {path}")
                    
                    # Try to read the first CSV file if available
                    if csv_files:
                        test_file = os.path.join(path, csv_files[0])
                        logger.info(f"Testing file read for {test_file}")
                        
                        try:
                            # Read the file
                            file_data = await asyncio.wait_for(
                                sftp.open(test_file, 'rb'),
                                timeout=5.0  # 5 second timeout for file open
                            )
                            
                            # Read first 1024 bytes
                            first_chunk = await file_data.read(1024)
                            file_data.close()
                            
                            # Check if we got any data
                            if first_chunk:
                                logger.info(f"Successfully read {len(first_chunk)} bytes from {test_file}")
                                logger.debug(f"First 100 bytes: {first_chunk[:100]}")
                            else:
                                logger.warning(f"File {test_file} is empty")
                        except Exception as e:
                            logger.error(f"Error reading file {test_file}: {str(e)}")
                    else:
                        logger.warning(f"No CSV files found in {path} to test file reading")
                        
                except asyncssh.SFTPError as e:
                    logger.error(f"SFTP error listing directory {path}: {str(e)}")
                    return False
                except asyncio.TimeoutError:
                    logger.error(f"Timeout listing directory {path}")
                    return False
        except asyncio.TimeoutError:
            logger.error(f"Timeout connecting to {host}:{port}")
            return False
        except Exception as e:
            logger.error(f"Error connecting to SFTP server: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()
                logger.info("SFTP connection closed")
                
        return True
    except Exception as e:
        logger.error(f"Unexpected error in SFTP test: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def main():
    """Main async function"""
    logger.info("=== DIRECT SFTP TEST SCRIPT STARTING ===")
    
    # Prompt for SFTP details if not in environment
    if not all([
        os.environ.get("TEST_SFTP_HOST"),
        os.environ.get("TEST_SFTP_USERNAME"),
        os.environ.get("TEST_SFTP_PASSWORD")
    ]):
        logger.info("SFTP details not found in environment variables.")
        logger.info("Using default test values - customize by setting environment variables:")
        logger.info("TEST_SFTP_HOST, TEST_SFTP_PORT, TEST_SFTP_USERNAME,")
        logger.info("TEST_SFTP_PASSWORD, TEST_DEATHLOG_PATH")
    
    # Run the test
    result = await direct_sftp_test(TEST_SERVER_CONFIG)
    
    if result:
        logger.info("=== SFTP TEST COMPLETED SUCCESSFULLY ===")
    else:
        logger.error("=== SFTP TEST FAILED ===")

if __name__ == "__main__":
    asyncio.run(main())