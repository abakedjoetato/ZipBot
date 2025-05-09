"""
SFTP connection handler for Tower of Temptation PvP Statistics Bot

This module provides utilities for connecting to game servers via SFTP 
and retrieving log files. It includes:
1. Robust error handling with retries
2. Connection pooling for multi-server support
3. Timeouts for non-blocking operation
4. Proper resource cleanup
5. Multi-guild safe operation
"""
import os
import logging
import asyncio
import re
import io
import functools
import random
import stat
import time
import traceback
from typing import List, Dict, Any, Optional, Tuple, Union, BinaryIO, Set, Callable, Sequence
from datetime import datetime, timedelta
import paramiko
import asyncssh
from utils.async_utils import retryable

# Configure module-specific logger
logger = logging.getLogger(__name__)

# Global connection pool for connection reuse
CONNECTION_POOL: Dict[str, 'SFTPClient'] = {}
POOL_LOCK = asyncio.Lock()

# Track active operations to prevent resource conflicts
ACTIVE_OPERATIONS: Dict[str, Set[str]] = {}

# Track operation timeouts to cleanup stuck operations
OPERATION_TIMEOUTS: Dict[str, datetime] = {}

def with_operation_tracking(op_name: str, timeout_minutes: int = 5):
    """Decorator to track and prevent conflicting SFTP operations with timeout handling.

    This improved decorator tracks operations to prevent conflicts and also adds
    timeout handling to automatically clean up stuck operations after a timeout period.

    Args:
        op_name: Operation name prefix (file path or operation type)
        timeout_minutes: Number of minutes before operation times out and is forcibly cleaned up

    Returns:
        Decorated function that tracks operations to prevent conflicts
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            if not hasattr(self, 'server_id') or not self.server_id:
                logger.warning(f"Operation {op_name} attempted without server_id")
                return await func(self, *args, **kwargs)

            # Create unique operation ID
            path = ""
            if args:
                # Get first argument which is usually the path
                path = str(args[0]) if args[0] else ""
            elif kwargs:
                # Try common parameter names for paths
                for param_name in ['remote_path', 'path', 'directory']:
                    if param_name in kwargs and kwargs[param_name]:
                        path = str(kwargs[param_name])
                        break

            operation_id = f"{op_name}:{path}"
            timeout_key = f"{self.server_id}:{operation_id}"

            # Get or create tracking set for this server
            if self.server_id not in ACTIVE_OPERATIONS:
                ACTIVE_OPERATIONS[self.server_id] = set()

            # Check if operation is already in progress
            if operation_id in ACTIVE_OPERATIONS[self.server_id]:
                # Check if operation has timed out
                if timeout_key in OPERATION_TIMEOUTS:
                    if datetime.now() > OPERATION_TIMEOUTS[timeout_key]:
                        # Operation has timed out, clean it up and proceed
                        logger.warning(f"Operation {operation_id} for server {self.server_id} has timed out and will be forcibly cleaned up")
                        ACTIVE_OPERATIONS[self.server_id].discard(operation_id)
                        OPERATION_TIMEOUTS.pop(timeout_key, None)
                    else:
                        logger.warning(f"Operation {operation_id} already in progress for server {self.server_id}")
                        return None
                else:
                    logger.warning(f"Operation {operation_id} already in progress for server {self.server_id}")
                    return None

            # Add operation to tracking
            ACTIVE_OPERATIONS[self.server_id].add(operation_id)

            # Set timeout for this operation
            OPERATION_TIMEOUTS[timeout_key] = datetime.now() + timedelta(minutes=timeout_minutes)

            try:
                # Execute operation
                start_time = datetime.now()
                result = await func(self, *args, **kwargs)
                elapsed = (datetime.now() - start_time).total_seconds()

                # Log long-running operations for performance monitoring
                if elapsed > 5:  # Log operations taking more than 5 seconds
                    logger.warning(f"Long-running operation {operation_id} for server {self.server_id} took {elapsed:.2f}s")

                return result
            except Exception as e:
                # Log and reraise exception
                logger.error(f"Error during operation {operation_id} for server {self.server_id}: {str(e)}")
                raise
            finally:
                # Remove operation from tracking and clear timeout
                if self.server_id in ACTIVE_OPERATIONS:
                    ACTIVE_OPERATIONS[self.server_id].discard(operation_id)
                    if not ACTIVE_OPERATIONS[self.server_id]:
                        ACTIVE_OPERATIONS.pop(self.server_id, None)

                # Clear timeout
                OPERATION_TIMEOUTS.pop(timeout_key, None)

        return wrapper
    return decorator

async def cleanup_stale_connections(max_idle_time: int = 300):
    """Cleanup stale connections in the connection pool

    This function identifies and removes connections that have been idle for too long,
    preventing resource exhaustion in environments with many servers.

    Args:
        max_idle_time: Maximum idle time in seconds before connection is considered stale
    """
    stale_connections = []

    # Find stale connections
    async with POOL_LOCK:
        now = datetime.now()
        for connection_id, client in list(CONNECTION_POOL.items()):
            # Check if connection is stale based on last activity
            if hasattr(client, 'last_activity'):
                idle_time = (now - client.last_activity).total_seconds()
                if idle_time > max_idle_time:
                    logger.info(f"Connection {connection_id} idle for {idle_time:.1f}s, marking as stale")
                    stale_connections.append(connection_id)
                elif idle_time > 60:  # Just log connections idle for more than a minute
                    logger.debug(f"Connection {connection_id} idle for {idle_time:.1f}s")

    # Disconnect and remove stale connections
    for connection_id in stale_connections:
        try:
            async with POOL_LOCK:
                if connection_id in CONNECTION_POOL:
                    client = CONNECTION_POOL[connection_id]
                    logger.info(f"Cleaning up stale connection: {connection_id}")
                    await client.disconnect()
                    CONNECTION_POOL.pop(connection_id, None)
        except Exception as e:
            logger.error(f"Error cleaning up stale connection {connection_id}: {e}")

async def cleanup_stuck_operations(max_stuck_time: int = 300):
    """Cleanup stuck operations that haven't completed within the timeout

    This function identifies operations that have been running for too long
    and forcibly removes them from tracking, allowing new operations to proceed.

    Args:
        max_stuck_time: Maximum time in seconds before operation is considered stuck
    """
    # Find and clean up stuck operations
    now = datetime.now()
    stuck_operations = []

    # Find stuck operations
    for timeout_key, timeout_time in list(OPERATION_TIMEOUTS.items()):
        if now > timeout_time:
            stuck_operations.append(timeout_key)

    # Clean up stuck operations
    cleanup_count = 0
    for timeout_key in stuck_operations:
        try:
            server_id, operation_id = timeout_key.split(':', 1)

            if server_id in ACTIVE_OPERATIONS and operation_id in ACTIVE_OPERATIONS[server_id]:
                logger.warning(f"Cleaning up stuck operation: {operation_id} for server {server_id}")
                ACTIVE_OPERATIONS[server_id].discard(operation_id)

                if not ACTIVE_OPERATIONS[server_id]:
                    ACTIVE_OPERATIONS.pop(server_id, None)

                cleanup_count += 1

            # Remove timeout tracking
            OPERATION_TIMEOUTS.pop(timeout_key, None)

        except Exception as e:
            logger.error(f"Error cleaning up stuck operation {timeout_key}: {e}")

    if cleanup_count > 0:
        logger.info(f"Cleaned up {cleanup_count} stuck operations")

async def periodic_connection_maintenance(interval: int = 60):
    """Periodically maintain SFTP connection pool

    This background task runs periodically to clean up stale connections and
    stuck operations, ensuring the system remains responsive under load.

    Args:
        interval: Cleanup interval in seconds
    """
    logger.info(f"Starting periodic connection maintenance task (interval: {interval}s)")

    while True:
        try:
            # Delay is at the beginning so we can safely use continue
            await asyncio.sleep(interval)

            # Skip if no connections or operations
            if not CONNECTION_POOL and not OPERATION_TIMEOUTS:
                continue

            # Log current state
            logger.debug(f"Connection pool: {len(CONNECTION_POOL)} connections, "
                         f"Active operations: {sum(len(ops) for ops in ACTIVE_OPERATIONS.values())}, "
                         f"Operation timeouts: {len(OPERATION_TIMEOUTS)}")

            # Cleanup stale connections
            await cleanup_stale_connections()

            # Cleanup stuck operations
            await cleanup_stuck_operations()

        except asyncio.CancelledError:
            # Allow clean shutdown
            logger.info("Connection maintenance task cancelled")
            break
        except Exception as e:
            # Log error but don't stop the maintenance task
            logger.error(f"Error in connection maintenance: {e}")
            traceback.print_exc()

async def get_sftp_client(
    hostname: Optional[str] = None,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    timeout: int = 30,
    max_retries: int = 3,
    server_id: Optional[str] = None,
    force_new: bool = False,
    # Support for alternative parameter naming
    sftp_host: Optional[str] = None,
    sftp_port: Optional[int] = None,
    sftp_username: Optional[str] = None,
    sftp_password: Optional[str] = None,
    original_server_id: Optional[str] = None,  # Added support for original server ID
    **kwargs  # For additional parameters
) -> 'SFTPClient':
    """Get SFTP client from connection pool or create new one.

    This factory function supports flexible parameter naming for backward compatibility.

    Args:
        hostname: SFTP hostname (primary parameter)
        port: SFTP port (primary parameter)
        username: SFTP username (primary parameter)
        password: SFTP password (primary parameter)
        timeout: Connection timeout in seconds
        max_retries: Maximum number of connection retries
        server_id: Server ID for tracking
        force_new: Force creation of new connection
        sftp_host: Alternative parameter for hostname
        sftp_port: Alternative parameter for port
        sftp_username: Alternative parameter for username
        sftp_password: Alternative parameter for password
        **kwargs: Additional parameters

    Returns:
        SFTPClient instance
    """
    # Handle parameter normalizing - prioritize the primary params but accept alternative naming
    combined_host = hostname or sftp_host or kwargs.get('host')
    initial_port = port or sftp_port or kwargs.get('port') or 22

    # If hostname has port embedded as hostname:port, extract it
    if combined_host and ":" in combined_host:
        hostname_parts = combined_host.split(":")
        host = hostname_parts[0]  # Extract just the hostname part
        if len(hostname_parts) > 1 and hostname_parts[1].isdigit():
            port_num = int(hostname_parts[1])  # Use the port from the combined string
            logger.info(f"Factory function split hostname:port format: {combined_host} -> hostname: {host}, port: {port_num}")
        else:
            host = combined_host
            port_num = initial_port
    else:
        host = combined_host
        port_num = initial_port

    user = username or sftp_username or kwargs.get('user')
    pwd = password or sftp_password or kwargs.get('pwd')

    # Create connection key
    if not server_id:
        logger.warning("SFTP client requested without server_id, this prevents proper isolation")
        # Generate a unique ID to avoid conflicts
        server_id = f"anonymous_{random.randint(1000, 9999)}"

    conn_key = f"{host}:{port_num}:{user}:{server_id}"

    async with POOL_LOCK:
        if not force_new and conn_key in CONNECTION_POOL:
            client = CONNECTION_POOL[conn_key]
            if await client.check_connection():
                logger.debug(f"Reusing existing SFTP connection: {conn_key}")
                return client
            else:
                logger.warning(f"Existing connection is invalid, creating new: {conn_key}")
                # Remove invalid connection
                CONNECTION_POOL.pop(conn_key, None)

    # Create new client with all parameters for maximum flexibility
    client = SFTPClient(
        hostname=host, 
        port=port_num, 
        username=user, 
        password=pwd, 
        timeout=timeout, 
        max_retries=max_retries, 
        server_id=server_id,
        original_server_id=original_server_id,  # Pass original server ID to client
        sftp_host=host,
        sftp_port=port_num,
        sftp_username=user,
        sftp_password=pwd
    )
    connected = await client.connect()

    if connected:
        # Add to pool
        async with POOL_LOCK:
            CONNECTION_POOL[conn_key] = client

        return client
    else:
        raise ConnectionError(f"Failed to establish SFTP connection to {host}:{port_num}")

class SFTPManager:
    """Manager for SFTP connections with high-level operations

    This class wraps SFTPClient to provide a simpler interface for common operations
    and handles proper connection management for multiple server contexts.
    """

    def __init__(
        self,
        hostname: str,
        port: int = 22,
        username: str = None,
        password: str = None,
        timeout: int = 30,
        max_retries: int = 3,
        server_id: Optional[str] = None,
        # Support alternative parameter naming
        sftp_host: Optional[str] = None,
        sftp_port: Optional[int] = None,
        sftp_username: Optional[str] = None,
        sftp_password: Optional[str] = None,
        original_server_id: Optional[str] = None,
    ):
        """Initialize SFTP manager

        Args:
            hostname: SFTP hostname (can include port as hostname:port)
            port: SFTP port
            username: SFTP username
            password: SFTP password
            timeout: Connection timeout in seconds
            max_retries: Maximum number of connection retries
            server_id: Server ID for tracking (standardized format)
            sftp_host: Alternative parameter for hostname
            sftp_port: Alternative parameter for port
            sftp_username: Alternative parameter for username
            sftp_password: Alternative parameter for password
            original_server_id: Original, unstandardized server ID for path construction
        """
        # Handle combined hostname:port format
        combined_hostname = hostname or sftp_host
        initial_port = port or sftp_port or 22

        # Parse hostname:port format if present
        if combined_hostname and ":" in combined_hostname:
            hostname_parts = combined_hostname.split(":")
            clean_hostname = hostname_parts[0]
            if len(hostname_parts) > 1 and hostname_parts[1].isdigit():
                extracted_port = int(hostname_parts[1])
                logger.info(f"SFTPManager split hostname:port format: {combined_hostname} -> hostname: {clean_hostname}, port: {extracted_port}")
                self.hostname = clean_hostname
                self.port = extracted_port
            else:
                self.hostname = combined_hostname
                self.port = initial_port
        else:
            self.hostname = combined_hostname
            self.port = initial_port

        self.username = username or sftp_username
        self.password = password or sftp_password
        self.timeout = timeout
        self.max_retries = max_retries
        self.server_id = server_id
        
        # Store numeric server ID for path construction - CRITICAL for correct folder paths
        # This is a high-priority fix to solve the UUID vs numeric ID path issue
        
        # ALWAYS get a numeric ID for path construction - critical fix for UUID path issues
        try:
            from utils.server_identity import identify_server, KNOWN_SERVERS
            
            # Start with reasonable default: provided original_server_id or server_id (will be improved)
            self.original_server_id = original_server_id or server_id
            
            # Priority 1: Check KNOWN_SERVERS mapping for server_id (highest authority)
            # This is the authoritative source for numeric IDs, and should be used whenever possible
            if server_id and server_id in KNOWN_SERVERS:
                numeric_id = KNOWN_SERVERS[server_id]
                logger.info(f"SFTPClient using known numeric ID '{numeric_id}' for path construction instead of '{server_id}'")
                self.original_server_id = numeric_id
                
            # Priority 2: If original_server_id is provided and is numeric, use it directly
            # This is likely a numeric ID explicitly provided by the caller
            elif original_server_id and str(original_server_id).isdigit():
                logger.info(f"SFTPClient using provided numeric original_server_id: {original_server_id}")
                # Already set in default above
                
            # Priority 3: If original_server_id is provided but is a UUID, try to map it
            # This handles the case where we're given a UUID as original_server_id
            elif original_server_id and len(str(original_server_id)) > 10:
                # First check if this UUID is in KNOWN_SERVERS
                if original_server_id in KNOWN_SERVERS:
                    numeric_id = KNOWN_SERVERS[original_server_id]
                    logger.info(f"SFTPClient mapped UUID original_server_id '{original_server_id}' to numeric ID '{numeric_id}'")
                    self.original_server_id = numeric_id
                else:
                    # Use server_identity module to resolve the UUID
                    # This will check all possible ways to determine the numeric server ID
                    numeric_id, is_known = identify_server(
                        server_id=original_server_id or "",
                        hostname=hostname or "",
                        server_name=server_id or ""  # Use server_id as fallback server_name
                    )
                    
                    if numeric_id:
                        logger.info(f"SFTPClient resolved UUID '{original_server_id}' to numeric ID '{numeric_id}'")
                        self.original_server_id = numeric_id
                    else:
                        # Extract digits from UUID as last resort
                        uuid_digits = ''.join(filter(str.isdigit, str(original_server_id)))
                        if uuid_digits:
                            extracted_id = uuid_digits[-5:] if len(uuid_digits) >= 5 else uuid_digits
                            logger.warning(f"SFTPClient extracting numeric ID from UUID: {extracted_id}")
                            self.original_server_id = extracted_id
            
            # 4. Priority 4: If we have a non-UUID server_id, use it directly if numeric
            elif server_id and str(server_id).isdigit():
                logger.info(f"SFTPClient using numeric server_id directly: {server_id}")
                self.original_server_id = server_id
                
            # 5. Priority 5: No direct numeric ID found, use server_identity to find one
            else:
                # Use all available information for best match
                numeric_id, is_known = identify_server(
                    server_id=server_id,
                    hostname=hostname,
                    server_name="",  # No name available at this level
                    guild_id=None  # No guild ID available at this level
                )
                
                if numeric_id:
                    logger.info(f"SFTPClient using identified numeric ID '{numeric_id}' for path construction")
                    self.original_server_id = numeric_id
                else:
                    # Last resort: Extract digits from server_id
                    uuid_digits = ''.join(filter(str.isdigit, str(server_id)))
                    if uuid_digits:
                        extracted_id = uuid_digits[-5:] if len(uuid_digits) >= 5 else uuid_digits
                        logger.warning(f"SFTPClient emergency fallback: using digits from server_id: {extracted_id}")
                        self.original_server_id = extracted_id
                    else:
                        # Absolute last resort: Use random numeric ID
                        import random
                        fallback_id = str(random.randint(10000, 99999))
                        logger.error(f"SFTPClient could not determine any numeric ID, using random fallback: {fallback_id}")
                        self.original_server_id = fallback_id
            
            # Ensure original_server_id is a string
            self.original_server_id = str(self.original_server_id)
            logger.info(f"Using original server ID '{self.original_server_id}' for path construction")
            
        except (ImportError, Exception) as e:
            # If server_identity module import fails, set a safe default
            logger.error(f"Error in server identity resolution: {e}")
            
            # Set a fallback ID
            if original_server_id:
                self.original_server_id = original_server_id
            elif server_id and str(server_id).isdigit():
                self.original_server_id = server_id
            else:
                # Extract numeric parts from server_id
                uuid_digits = ''.join(filter(str.isdigit, str(server_id)))
                if uuid_digits:
                    self.original_server_id = uuid_digits[-5:] if len(uuid_digits) >= 5 else uuid_digits
                else:
                    # Last resort random numeric ID
                    import random
                    self.original_server_id = str(random.randint(10000, 99999))
            
            logger.warning(f"Using fallback original_server_id: {self.original_server_id}")
            logger.warning(f"Could not import server_identity module, falling back to basic ID resolution: {e}")
            self.original_server_id = original_server_id or server_id
        
        # Ensure original_server_id is a string 
        if self.original_server_id is not None:
            self.original_server_id = str(self.original_server_id)
        
        # Always log the server ID being used for path construction
        if self.original_server_id != server_id:
            logger.info(f"Using original server ID '{self.original_server_id}' for path construction instead of standardized ID '{server_id}'")
        else:
            logger.info(f"Using server ID '{server_id}' for path construction")
        self.client = None
        self.last_error = None
        
    @property
    def is_connected(self) -> bool:
        """Check if the client is connected and ready for operations

        Returns:
            bool: True if connected and ready, False otherwise
        """
        if self.client is None:
            return False
        
        # If the client has an is_connected property, use it
        if hasattr(self.client, 'is_connected'):
            return self.client.is_connected
            
        # Fallback for older clients
        return self.client._connected if hasattr(self.client, '_connected') else False

    async def connect(self) -> 'SFTPManager':
        """Establish connection to SFTP server

        Returns:
            SFTPManager: The manager instance for method chaining.
            Check is_connected property to determine if connection was successful.
        """
        try:
            self.client = await get_sftp_client(
                hostname=self.hostname,
                port=self.port,
                username=self.username,
                password=self.password,
                timeout=self.timeout,
                max_retries=self.max_retries,
                server_id=self.server_id,
                original_server_id=self.original_server_id  # Pass original server ID to client
            )
            # Return self (manager) for method chaining, not the client
            return self
        except Exception as e:
            self.last_error = str(e)
            logger.error(f"Failed to connect to SFTP server {self.hostname}:{self.port}: {e}")
            # Set client to None to indicate connection failure
            self.client = None
            # Still return self (manager) for consistent method chaining
            return self

    # Delegation methods to forward calls to the client
    async def exists(self, path: str) -> bool:
        """Check if a path exists

        Args:
            path: Path to check

        Returns:
            bool: True if path exists, False otherwise
        """
        if not self.client:
            logger.error(f"SFTP client is missing when trying to check if path exists: {path}")
            return False

        try:
            return await self.client.exists(path)
        except Exception as e:
            logger.error(f"Failed to check if path exists {path}: {e}")
            return False

    async def find_files_by_pattern(self, directory: str, pattern: str, recursive: bool = False, max_depth: int = 5) -> List[str]:
        """Find files matching a pattern in a directory

        Args:
            directory: Directory to search in
            pattern: Regex pattern to match filenames
            recursive: Whether to search recursively
            max_depth: Maximum recursion depth

        Returns:
            List[str]: List of matching file paths
        """
        if not self.client:
            logger.error(f"SFTP client is missing when trying to find files by pattern")
            return []

        try:
            return await self.client.find_files_by_pattern(directory, pattern, recursive, max_depth)
        except Exception as e:
            logger.error(f"Failed to find files by pattern {pattern} in {directory}: {e}")
            return []

    async def find_files_recursive(self, directory: str, pattern: str, result: List[str], recursive: bool = False, max_depth: int = 5) -> None:
        """Find files recursively in a directory

        Args:
            directory: Directory to search in
            pattern: Regex pattern to match filenames
            result: List to append results to
            recursive: Whether to search recursively
            max_depth: Maximum recursion depth
        """
        if not self.client:
            logger.error(f"SFTP client is missing when trying to find files recursively")
            return

        try:
            await self.client.find_files_recursive(directory, pattern, result, recursive, max_depth)
        except Exception as e:
            logger.error(f"Failed to find files recursively with pattern {pattern} in {directory}: {e}")

    async def get_file_info(self, path: str) -> Optional[Dict[str, Any]]:
        """Get information about a file

        Args:
            path: Path to file

        Returns:
            Optional[Dict[str, Any]]: File information if found, None otherwise
        """
        if not self.client:
            logger.error(f"SFTP client is missing when trying to get file info: {path}")
            return None
            
        try:
            return await self.client.get_file_info(path)
        except Exception as e:
            logger.error(f"Failed to get file info for {path}: {e}")
            return None
            
    async def is_file(self, path: str) -> bool:
        """Check if a path is a file (not a directory) on the SFTP server

        Args:
            path: Path to check

        Returns:
            bool: True if path is a file, False otherwise
        """
        if not self.client:
            logger.error(f"SFTP client is missing when trying to check if path is a file: {path}")
            return False

        try:
            # Use client's is_file method if available
            if hasattr(self.client, 'is_file'):
                return await self.client.is_file(path)
            
            # Otherwise use get_file_info to determine if it's a file
            file_info = await self.get_file_info(path)
            if file_info and isinstance(file_info, dict):
                return file_info.get("is_file", False)
            
            return False
        except Exception as e:
            logger.warning(f"Failed to check if path is a file {path}: {e}")
            return False

    async def download_file(self, remote_path: str, local_path: Optional[str] = None) -> Optional[bytes]:
        """Download a file from the SFTP server

        Args:
            remote_path: Remote file path
            local_path: Optional local file path to save to

        Returns:
            Optional[bytes]: File content if local_path is not provided, None otherwise
        """
        if not self.client:
            logger.error(f"SFTP client is missing when trying to download file: {remote_path}")
            return None

        try:
            return await self.client.download_file(remote_path, local_path)
        except Exception as e:
            logger.error(f"Failed to download file {remote_path}: {e}")
            return None

    async def list_files(self, directory: str, pattern: Optional[str] = None) -> List[str]:
        """List files in a directory

        Args:
            directory: Directory to list
            pattern: Optional regex pattern to filter filenames

        Returns:
            List[str]: List of filenames
        """
        if not self.client:
            logger.error(f"SFTP client is missing when trying to list files: {directory}")
            return []

        try:
            if hasattr(self.client, 'list_files'):
                return await self.client.list_files(directory, pattern)
            elif pattern is None and hasattr(self.client, '_sftp_client') and hasattr(self.client._sftp_client, 'listdir'):
                return await self.client._sftp_client.listdir(directory)
            else:
                # Fallback to find_files_by_pattern
                return await self.client.find_files_by_pattern(directory, pattern or '.*', recursive=False)
        except Exception as e:
            logger.error(f"Failed to list files in {directory}: {e}")
            return []
            
    async def listdir(self, directory: str) -> List[str]:
        """List all files and directories in a directory (compatibility method)
        
        Args:
            directory: Directory to list
            
        Returns:
            List[str]: List of filenames and directory names
        """
        # This is a compatibility method that delegates to list_files
        return await self.list_files(directory)

    async def read_file(self, remote_path: str, start_line: int = 0, max_lines: int = -1) -> Optional[List[str]]:
        """Read a file from the SFTP server

        Args:
            remote_path: Remote file path
            start_line: Line to start reading from (0-based)
            max_lines: Maximum number of lines to read (-1 for all)

        Returns:
            Optional[List[str]]: File content as list of lines
        """
        if not self.client:
            logger.error(f"SFTP client is missing when trying to read file: {remote_path}")
            return None

        try:
            return await self.client.read_file(remote_path, start_line, max_lines)
        except Exception as e:
            logger.error(f"Failed to read file {remote_path}: {e}")
            return None

    async def read_csv_lines(self, remote_path: str, encoding: str = 'utf-8', fallback_encodings: Optional[List[str]] = None) -> Optional[List[str]]:
        """Read CSV lines from a remote file

        Args:
            remote_path: Path to CSV file
            encoding: Encoding to use
            fallback_encodings: List of encodings to try if the primary encoding fails

        Returns:
            Optional[List[str]]: List of CSV lines
        """
        if not self.client:
            logger.error(f"SFTP client is missing when trying to read CSV lines: {remote_path}")
            return None

        try:
            return await self.client.read_csv_lines(remote_path, encoding, fallback_encodings)
        except Exception as e:
            logger.error(f"Failed to read CSV lines from {remote_path}: {e}")
            return None

    async def get_file_stats(self, path: str) -> Optional[Any]:
        """Get file stats (modification time, size, etc.)

        Args:
            path: Path to file

        Returns:
            Optional[Any]: File stats object or None if file not found
        """
        if not self.client:
            logger.error(f"SFTP client is missing when trying to get file stats: {path}")
            return None

        try:
            # Try to get file stats - first try through stat if available
            if hasattr(self.client, 'stat'):
                return await self.client.stat(path)

            # Fallback to get_file_info method
            file_info = await self.get_file_info(path)
            if file_info:
                # Convert file_info to a stat-like object
                return file_info

            return None
        except Exception as e:
            logger.error(f"Failed to get file stats for {path}: {e}")
            return None

    async def disconnect(self) -> None:
        """Disconnect from SFTP server with proper resource cleanup

        This ensures all resources are properly released and connections
        are cleanly closed to prevent resource leaks or hanging connections.
        """
        try:
            if self.client:
                logger.debug(f"Disconnecting SFTP client for {self.hostname}:{self.port}")
                # Ensure we don't lose references to the client during disconnect
                client = self.client
                # Clear the reference first to prevent recursive issues
                self.client = None

                # Now execute the actual disconnect
                try:
                    await client.disconnect()
                except Exception as e:
                    logger.warning(f"Error during client disconnect: {str(e)}")

                # Additional cleanup
                logger.debug("SFTP disconnect completed successfully")
        except Exception as e:
            logger.error(f"Error in disconnect cleanup: {str(e)}")
            # Still set client to None even if there was an error
            self.client = None

    async def get_log_file(self, server_dir: Optional[str] = None, base_path: Optional[str] = None) -> Optional[str]:
        """Get the Deadside.log file path using multiple search strategies

        This method tries multiple path formats to locate the log file:
        1. Standard path: /{hostname}_{server_id}/Logs/Deadside.log (MOST COMMON)
        2. Direct path: /Logs/Deadside.log
        
        Args:
            server_dir: Optional pre-constructed server directory (e.g., "hostname_serverid")
            base_path: Optional base path override for finding the log file
        3. Server name path: /{server_id}/Logs/Deadside.log
        4. Recursive search from root for Deadside.log (limited depth)

        Returns:
            Optional[str]: Path to Deadside.log if found, None otherwise
        """
        # Check initial connection status
        was_connected = self.client is not None
        logger.debug(f"Initial connection status: {was_connected}")

        # Ensure connection is active
        if not was_connected:
            if not await self.connect():
                logger.error("Could not establish SFTP connection when trying to get log file")
                return None

        # Track all paths we've tried for diagnostics
        tried_paths = []
        max_attempts = 3

        for attempt in range(1, max_attempts + 1):
            # Check if connection was lost during operation
            if not self.client:
                logger.warning(f"Connection lost during get_log_file (attempt {attempt}/{max_attempts}), reconnecting...")
                if not await self.connect():
                    logger.error(f"Failed to reconnect on attempt {attempt}/{max_attempts}")
                    if attempt == max_attempts:
                        logger.error("Max reconnection attempts reached, giving up")
                        return None
                    await asyncio.sleep(1)  # Brief delay before retry
                    continue

            try:
                # Clean hostname for directory structure (remove port if present)
                clean_hostname = self.hostname.split(':')[0] if self.hostname else "server"
                
                # HIGH PRIORITY FIX: Ensure we always use the numeric server ID (original_server_id) for path construction
                # This is critical for consistent path generation across server UUIDs
                
                # PRIORITY 1: Always use original_server_id if available and numeric
                path_server_id = None
                if hasattr(self, 'original_server_id') and self.original_server_id:
                    if str(self.original_server_id).isdigit():
                        path_server_id = self.original_server_id
                        logger.info(f"Using numeric original_server_id '{path_server_id}' for path construction")
                    else:
                        # Not numeric, but still prefer original_server_id over server_id
                        path_server_id = self.original_server_id
                        logger.debug(f"Using non-numeric original_server_id '{path_server_id}' for path construction")
                
                # PRIORITY 2: Try to extract numeric ID from hostname if path_server_id isn't set yet
                if not path_server_id and '_' in clean_hostname:
                    potential_id = clean_hostname.split('_')[-1]
                    if potential_id.isdigit():
                        path_server_id = potential_id
                        logger.info(f"Using numeric ID from hostname: {path_server_id}")
                
                # PRIORITY 3: Last resort - use server_id but log a warning since this is likely a UUID
                if not path_server_id:
                    path_server_id = self.server_id
                    logger.warning(f"No numeric server ID found for path construction, using UUID as fallback: {path_server_id}")
                
                # Always convert to string for path construction
                server_id_str = str(path_server_id)

                # Define path structure constants
                LOG_DIR = "Logs"
                DEATHLOG_DIR = "deathlogs"
                ACTUAL1_DIR = "actual1"

                # First path is always the correct Tower of Temptation path structure
                base_path = os.path.join("/", f"{clean_hostname}_{server_id_str}")

                # Define paths for different file types
                log_path = os.path.join(base_path, LOG_DIR)
                csv_path = os.path.join(base_path, ACTUAL1_DIR, DEATHLOG_DIR)

                paths_to_try = [
                    # Primary path: /hostname_serverid/Logs/Deadside.log
                    os.path.join(log_path, "Deadside.log"),

                    # Fallback paths only if primary fails
                    os.path.join("/", "Logs", "Deadside.log"),
                    os.path.join(base_path, "logs", "Deadside.log"),

                    # Strategy 7: Simple root-based path
                    os.path.join("/Deadside.log"),

                    # Strategy 8: Actual1 directory (sometimes used)
                    os.path.join("/", f"{clean_hostname}_{server_id_str}", "actual1", "Logs", "Deadside.log"),

                    # Strategy 9: Game directory structure with server ID
                    os.path.join("/", server_id_str, "game", "Logs", "Deadside.log"),

                    # Strategy 10: Game directory structure with hostname
                    os.path.join("/", clean_hostname, "game", "Logs", "Deadside.log")
                ]

                # Try each path in sequence
                for path in paths_to_try:
                    if path in tried_paths:
                        continue  # Skip paths we've already tried

                    tried_paths.append(path)
                    try:
                        # Verify connection before each attempt
                        if not self.client:
                            logger.warning(f"Connection lost before checking path: {path}, reconnecting...")
                            if not await self.connect():
                                logger.error("Failed to reconnect, skipping this path")
                                continue

                        logger.debug(f"SFTPManager checking for log file at: {path}")
                        file_exists = False

                        # Try different methods to check file existence
                        try:
                            if hasattr(self.client, 'stat'):
                                await self.client.stat(path)
                                file_exists = True
                            elif hasattr(self.client, 'get_file_info'):
                                info = await self.client.get_file_info(path)
                                file_exists = bool(info)
                        except Exception as path_err:
                            logger.debug(f"File check failed at {path}: {path_err}")
                            continue  # Try next path

                        if file_exists:
                            logger.info(f"Found Deadside.log at: {path}")
                            return path
                    except Exception as e:
                        logger.debug(f"Log file not found at {path}: {str(e)}")
                        # Continue trying other paths
                        continue

                # If no success with predefined paths, try directory listings
                if not any(p.endswith('Deadside.log') for p in tried_paths if p in paths_to_try):
                    logger.info("Trying to find Deadside.log through directory listings")

                    # Create a list of directories to search
                    dirs_to_search = [
                        os.path.join("/", f"{clean_hostname}_{server_id_str}", "Logs"),
                        os.path.join("/", "Logs"),
                        os.path.join("/", server_id_str, "Logs"),
                        os.path.join("/", clean_hostname, "Logs"),
                        os.path.join("/", f"{clean_hostname}_{server_id_str}", "logs"),
                        "/logs"
                    ]

                    for dir_path in dirs_to_search:
                        if not self.client:
                            if not await self.connect():
                                logger.error("Connection lost during directory search")
                                break

                        try:
                            logger.debug(f"Listing files in directory: {dir_path}")
                            files = await self.list_files(dir_path, r"Deadside\.log")
                            if files:
                                full_path = os.path.join(dir_path, files[0])
                                logger.info(f"Found Deadside.log through directory listing: {full_path}")
                                return full_path
                        except Exception as list_err:
                            logger.debug(f"Error listing files in {dir_path}: {list_err}")

                # If still no success, try recursive search (last resort)
                logger.info("Attempting to search for Deadside.log recursively (limited depth)")
                try:
                    # Check connection before recursive search
                    if not self.client:
                        logger.warning("Connection lost before recursive search, reconnecting...")
                        if not await self.connect():
                            logger.error("Failed to reconnect for recursive search")
                            if attempt < max_attempts:
                                continue  # Try again in next attempt
                            else:
                                return None  # Give up

                    # Try a recursive search from root with limited depth
                    # First check if the client has the find_files_recursive method
                    if hasattr(self.client, 'find_files_recursive'):
                        result = []
                        await self.client.find_files_recursive("/", r"Deadside\.log", result, recursive=True, max_depth=6)

                        if result:
                            logger.info(f"Found Deadside.log through recursive search: {result[0]}")
                            return result[0]
                    elif hasattr(self.client, 'find_files_by_pattern'):
                        # Use find_files_by_pattern which is definitely available in SFTPClient
                        logger.info("Using find_files_by_pattern for recursive search")
                        result = await self.client.find_files_by_pattern("/", r"Deadside\.log", recursive=True, max_depth=10)

                        if result:
                            logger.info(f"Found Deadside.log through pattern search: {result[0]}")
                            return result[0]
                except Exception as search_error:
                    logger.warning(f"Recursive search for Deadside.log failed: {search_error}")

                # If we get here and we're on the last attempt, we didn't find the file
                if attempt == max_attempts:
                    logger.warning(f"Deadside.log not found in any of the tried paths after {max_attempts} attempts")
                    logger.info(f"Paths tried: {tried_paths}")
                    return None

                # If not the last attempt, wait before trying again
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Failed to get log file on attempt {attempt}/{max_attempts}: {e}")
                if attempt == max_attempts:
                    return None

                # If connection was lost, try to reconnect for next attempt
                if not self.client:
                    await self.connect()

                # Brief delay before retry
                await asyncio.sleep(1)

        # If we reach here, all attempts failed
        return None

    async def list_files(self, directory: str = "/logs", pattern: str = r".*\.csv") -> List[str]:
        """List files in directory matching pattern

        Args:
            directory: Directory to list files from
            pattern: Regex pattern to match files against

        Returns:
            List[str]: List of matching file paths (empty list if error)
        """
        # Validate inputs
        if not directory:
            logger.warning("Directory parameter is missing in list_files")
            directory = "/logs"

        # Ensure client is connected
        if not self.client:
            logger.info(f"Creating new SFTP client connection for list_files({directory})")
            if not await self.connect():
                logger.error(f"Failed to establish SFTP connection for list_files({directory})")
                return []

        # Retry logic for better reliability
        max_attempts = 2
        for attempt in range(1, max_attempts + 1):
            try:
                # Use find_files_by_pattern which already has proper error handling
                files = await self.client.find_files_by_pattern(directory, pattern)

                # Always return a list, even if files is empty
                return files if files else []

            except Exception as e:
                self.last_error = str(e)
                logger.error(f"Error listing files in {directory} (attempt {attempt}/{max_attempts}): {e}")

                # If this isn't the last attempt, try reconnecting
                if attempt < max_attempts:
                    logger.info(f"Attempting to reconnect for list_files retry ({attempt}/{max_attempts})")
                    await self.disconnect()
                    await asyncio.sleep(1)  # Brief delay before retry
                    await self.connect()

        # If we get here, all attempts failed
        logger.warning(f"All attempts to list files in {directory} failed")
        return []

    async def read_file(self, path: str) -> Optional[List[str]]:
        """Read file contents

        Args:
            path: File path to read

        Returns:
            Optional[List[str]]: File contents as list of lines or None if error
        """
        if not self.client:
            if not await self.connect():
                logger.error(f"SFTP client is missing when trying to read file {path}")
                return None

        try:
            return await self.client.read_file(path)
        except Exception as e:
            self.last_error = str(e)
            logger.error(f"Error reading file {path}: {e}")
            return None

    async def find_csv_files(self, directory: str = "/logs", 
                             date_range: Optional[Tuple[datetime, datetime]] = None,
                             recursive: bool = False,
                             max_depth: int = 3) -> List[str]:
        """Find CSV files in directory with enhanced error handling

        Args:
            directory: Directory to search
            date_range: Optional date range for filtering files
            recursive: Whether to search recursively in subdirectories
            max_depth: Maximum recursion depth (only used if recursive=True)

        Returns:
            List[str]: List of CSV file paths sorted by date
        """
        # Validate inputs
        if not directory:
            logger.warning("Directory parameter is missing in find_csv_files")
            directory = "/logs"

        # Ensure client is connected
        if not self.client:
            logger.info(f"Creating new SFTP client connection for find_csv_files({directory})")
            if not await self.connect():
                logger.error(f"Failed to establish SFTP connection for find_csv_files({directory})")
                return []

        # Retry logic for better reliability
        max_attempts = 2
        for attempt in range(1, max_attempts + 1):
            try:
                # Call the client's find_csv_files method
                if recursive:
                    # Use recursive search if requested
                    logger.info(f"Using recursive search for CSV files in {directory} (max_depth={max_depth})")
                    files = await self.client._find_csv_files_recursive(directory, max_depth=max_depth)
                else:
                    # Use regular search
                    files = await self.client.find_csv_files(directory, date_range)

                # Handle None results safely
                if not files:
                    logger.warning(f"find_csv_files returned None for {directory}")
                    return []

                # Log success
                logger.info(f"Found {len(files)} CSV files in {directory}")
                return files

            except Exception as e:
                self.last_error = str(e)
                logger.error(f"Error finding CSV files in {directory} (attempt {attempt}/{max_attempts}): {e}")

                # If this isn't the last attempt, try reconnecting
                if attempt < max_attempts:
                    logger.info(f"Attempting to reconnect for find_csv_files retry ({attempt}/{max_attempts})")
                    await self.disconnect()
                    await asyncio.sleep(1)  # Brief delay before retry
                    await self.connect()

        # If we get here, all attempts failed
        logger.warning(f"All attempts to find CSV files in {directory} failed")
        return []

    async def read_csv_lines(self, path: str) -> List[str]:
        """Read CSV file lines with enhanced error handling and retries

        Args:
            path: File path to read

        Returns:
            List[str]: CSV file lines
        """
        # Validate input
        if not path:
            logger.error("Path parameter is empty in read_csv_lines")
            return []

        # Ensure client is connected
        if not self.client:
            logger.info(f"Creating new SFTP client connection for read_csv_lines({path})")
            if not await self.connect():
                logger.error(f"Failed to establish SFTP connection for read_csv_lines({path})")
                return []

        # Retry logic for better reliability
        max_attempts = 2
        for attempt in range(1, max_attempts + 1):
            try:
                # Call the client's read_csv_lines method
                lines = await self.client.read_csv_lines(path)

                # Handle None results safely
                if not lines:
                    logger.warning(f"read_csv_lines returned None for {path}")
                    return []

                # Log success
                logger.info(f"Read {len(lines)} lines from CSV file {path}")
                return lines

            except Exception as e:
                self.last_error = str(e)
                logger.error(f"Error reading CSV file {path} (attempt {attempt}/{max_attempts}): {e}")

                # If this isn't the last attempt, try reconnecting
                if attempt < max_attempts:
                    logger.info(f"Attempting to reconnect for read_csv_lines retry ({attempt}/{max_attempts})")
                    await self.disconnect()
                    await asyncio.sleep(1)  # Brief delay before retry
                    await self.connect()

        # If we get here, all attempts failed
        logger.warning(f"All attempts to read CSV file {path} failed")
        return []

    async def get_file_stats(self, path: str) -> Optional[Any]:
        """Get file statistics

        Args:
            path: File path to get stats for

        Returns:
            Optional[Any]: File stats object or None if error
        """
        if not self.client:
            if not await self.connect():
                logger.error(f"SFTP client is missing when trying to get file stats for {path}")
                return None

        try:
            # Get file info
            file_info = await self.client.get_file_info(path)

            # If file_info is missing, return None
            if not file_info:
                logger.warning(f"No file info returned for {path}")
                return None

            # If file_info is a dict, ensure it has st_mtime key
            if isinstance(file_info, dict):
                # Add st_mtime if it doesn't exist but mtime does
                if 'st_mtime' not in file_info and 'mtime' in file_info:
                    # Handle both datetime and timestamp formats
                    if isinstance(file_info['mtime'], datetime):
                        file_info['st_mtime'] = file_info['mtime'].timestamp()
                    else:
                        file_info['st_mtime'] = file_info['mtime']

                # Create a simple object to mimic os.stat_result
                class StatResult:
                    pass

                result = StatResult()
                for key, value in file_info.items():
                    setattr(result, key, value)

                return result

            # Return the original result if it's not a dict
            return file_info
        except Exception as e:
            self.last_error = str(e)
            logger.error(f"Error getting file stats for {path}: {e}")
            return None

    async def _download_to_memory(self, path: str) -> Optional[bytes]:
        """Helper method to download file to memory with multiple implementation strategies
        
        Args:
            path: Path to file on SFTP server
            
        Returns:
            bytes: File contents or None if error
        """
        if not self.client:
            logger.error("Not connected when trying to download to memory")
            return None
            
        # Try multiple methods to download file to memory
        try:
            # Special case for AsyncSSH - detect by module
            # This check avoids attribute errors when the client doesn't have expected methods
            try:
                import asyncssh
                if isinstance(self.client, asyncssh.SFTPClient):
                    logger.info(f"Detected AsyncSSH SFTP client, using optimized methods")
                    try:
                        # Try the direct AsyncSSH method for reading files
                        content = await self.client.readfile(path)
                        if isinstance(content, str):
                            content = content.encode('utf-8')
                        logger.info(f"Downloaded {path} using AsyncSSH readfile ({len(content)} bytes)")
                        return content
                    except Exception as ssh_err:
                        logger.warning(f"AsyncSSH readfile failed: {ssh_err}, trying other methods")
            except (ImportError, Exception) as e:
                # If asyncssh isn't available or there's another error, continue with other methods
                logger.debug(f"AsyncSSH special handling skipped: {e}")
            
            # Method 1: Use getfo if available (most efficient)
            if hasattr(self.client, 'getfo'):
                try:
                    file_obj = io.BytesIO()
                    await self.client.getfo(path, file_obj)
                    file_obj.seek(0)
                    content = file_obj.read()
                    logger.info(f"Downloaded {path} to memory using getfo ({len(content)} bytes)")
                    return content
                except Exception as getfo_err:
                    logger.warning(f"getfo method failed: {getfo_err}, trying alternatives")
            
            # Method 2: Use read if available
            if hasattr(self.client, 'read'):
                try:
                    content = await self.client.read(path)
                    logger.info(f"Downloaded {path} to memory using read ({len(content)} bytes)")
                    return content
                except Exception as read_err:
                    logger.warning(f"read method failed: {read_err}, trying alternatives")
                    
            # Method 3: Use open+read if available
            if hasattr(self.client, 'open'):
                try:
                    async with self.client.open(path, 'rb') as f:
                        content = await f.read()
                    logger.info(f"Downloaded {path} to memory using open+read ({len(content)} bytes)")
                    return content
                except Exception as open_err:
                    logger.warning(f"open method failed: {open_err}, trying alternatives")
                    
            # Method 4: Try other AsyncSSH-style methods by string matching on type
            try:
                # Check if this looks like an asyncssh SFTP client
                client_type = str(type(self.client)).lower()
                if 'asyncssh' in client_type or 'sftp' in client_type:
                    # Try some common methods that might be available
                    for method_name in ['readfile', 'getfile', 'download', 'read_file', 'fetch']:
                        if hasattr(self.client, method_name):
                            try:
                                logger.info(f"Trying alternative method: {method_name}")
                                
                                if method_name in ['readfile', 'read_file', 'fetch', 'download']:
                                    # Methods that return content directly
                                    content = await getattr(self.client, method_name)(path)
                                    if content:
                                        if isinstance(content, str):
                                            content = content.encode('utf-8') 
                                        logger.info(f"Downloaded using {method_name} ({len(content)} bytes)")
                                        return content
                                else:
                                    # Methods that write to a file-like object
                                    file_obj = io.BytesIO()
                                    await getattr(self.client, method_name)(path, file_obj)
                                    file_obj.seek(0)
                                    content = file_obj.read()
                                    if content:
                                        logger.info(f"Downloaded using {method_name} ({len(content)} bytes)")
                                        return content
                            except Exception as method_err:
                                logger.debug(f"Method {method_name} failed: {method_err}")
            except Exception as type_err:
                logger.debug(f"Type-based method detection failed: {type_err}")
            
            # Method 5: Use get with a temporary file (least efficient but reliable fallback)
            temp_file = f"temp_{int(time.time())}.tmp"
            try:
                await self.client.get(path, temp_file)
                
                # Read the temp file
                with open(temp_file, 'rb') as f:
                    content = f.read()
                
                # Clean up temp file
                try:
                    os.remove(temp_file)
                except Exception as rm_err:
                    logger.warning(f"Failed to remove temp file {temp_file}: {rm_err}")
                    
                logger.info(f"Downloaded {path} to memory using temp file ({len(content)} bytes)")
                return content
            except Exception as temp_err:
                logger.warning(f"Temp file method failed: {temp_err}")
                try:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                except:
                    pass
            
            # If we reach here, all methods failed
            logger.error(f"All download methods failed for {path}")
            return None
            
        except Exception as e:
            logger.error(f"Error downloading file {path} to memory: {e}")
            return None

    async def download_file(self, path: str) -> Optional[bytes]:
        """Download file contents with enhanced error handling and retries

        Args:
            path: File path to download

        Returns:
            Optional[bytes]: File contents as bytes or None if error
        """
        # Validate input
        if not path:
            logger.error("Path parameter is empty in download_file")
            return None

        # Ensure client is connected
        if not self.client:
            logger.info(f"Creating new SFTP client connection for download_file({path})")
            if not await self.connect():
                logger.error(f"Failed to establish SFTP connection for download_file({path})")
                return None

        # Retry logic for better reliability
        max_attempts = 2
        for attempt in range(1, max_attempts + 1):
            try:
                # Use the helper method that will try multiple download strategies
                data = await self._download_to_memory(path)
                
                # Handle None results safely
                if not data:
                    logger.warning(f"download_file returned None for {path}")
                    if attempt < max_attempts:
                        logger.info(f"Retrying download (attempt {attempt+1}/{max_attempts})...")
                        continue
                    return None

                # Log success
                logger.info(f"Downloaded {len(data)} bytes from file {path}")
                return data

            except Exception as e:
                self.last_error = str(e)
                logger.error(f"Error downloading file {path} (attempt {attempt}/{max_attempts}): {e}")

                # If this isn't the last attempt, try reconnecting
                if attempt < max_attempts:
                    logger.info(f"Attempting to reconnect for download_file retry ({attempt}/{max_attempts})")
                    await self.disconnect()
                    await asyncio.sleep(1)  # Brief delay before retry
                    await self.connect()

        # If we get here, all attempts failed
        logger.warning(f"All attempts to download file {path} failed")
        return None

class SFTPClient:
    """SFTP client for game servers

    Features:
    - Robust error handling with retries and timeouts
    - Connection pooling for efficient resource usage
    - Operation tracking to prevent conflicts
    - Automatic reconnection
    - Detailed error diagnostics
    """

    def __init__(
        self,
        hostname: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        server_id: Optional[str] = None,
        # Support for alternative parameter naming for backward compatibility
        sftp_host: Optional[str] = None,
        sftp_port: Optional[int] = None,
        sftp_username: Optional[str] = None,
        sftp_password: Optional[str] = None,
        original_server_id: Optional[str] = None,  # Added support for original server ID
        **kwargs  # Accept additional parameters for flexibility
    ):
        """Initialize SFTP handler with flexible parameter naming

        Args:
            hostname: SFTP hostname (primary parameter)
            port: SFTP port (primary parameter)
            username: SFTP username (primary parameter)
            password: SFTP password (primary parameter)
            timeout: Connection timeout in seconds
            max_retries: Maximum number of connection retries
            server_id: Server ID for multi-server tracking
            sftp_host: Alternative parameter for hostname (for backward compatibility)
            sftp_port: Alternative parameter for port (for backward compatibility)
            sftp_username: Alternative parameter for username (for backward compatibility)
            sftp_password: Alternative parameter for password (for backward compatibility)
            **kwargs: Additional parameters for future extensibility
        """
        # Handle alternative parameter naming conventions (backward compatibility)
        combined_hostname = hostname or sftp_host or kwargs.get('host')
        initial_port = port or sftp_port or kwargs.get('port') or 22

        # If hostname has port embedded as hostname:port, extract it
        if combined_hostname and ":" in combined_hostname:
            hostname_parts = combined_hostname.split(":")
            clean_hostname = hostname_parts[0]  # Extract just the hostname part
            if len(hostname_parts) > 1 and hostname_parts[1].isdigit():
                extracted_port = int(hostname_parts[1])  # Use the port from the combined string
                logger.info(f"Split hostname:port format: {combined_hostname} -> hostname: {clean_hostname}, port: {extracted_port}")
                self.hostname = clean_hostname
                self.port = extracted_port
            else:
                self.hostname = combined_hostname
                self.port = initial_port
        else:
            self.hostname = combined_hostname
            self.port = initial_port

        self.username = username or sftp_username or kwargs.get('user')
        self.password = password or sftp_password or kwargs.get('pwd')
        self.timeout = timeout
        self.max_retries = max_retries
        self._sftp_client = None
        self._ssh_client = None
        self._connected = False
        self._connection_attempts = 0
        self.server_id = str(server_id) if server_id else None
        
        # Store the original server ID for path construction - critical for correct folder paths
        # If original_server_id is provided, it will be used for path construction
        self.original_server_id = original_server_id or self.server_id
        
        # Ensure original_server_id is a string 
        if self.original_server_id is not None:
            self.original_server_id = str(self.original_server_id)
        
        # Always log the server ID being used for path construction
        if original_server_id and str(original_server_id) != str(server_id):
            logger.info(f"SFTPClient using original server ID '{original_server_id}' for path construction instead of '{server_id}'")
        else:
            logger.info(f"SFTPClient using server ID '{server_id}' for path construction")
        self.last_error = None
        self.last_operation = None
        self.connection_id = f"{self.hostname}:{self.port}:{self.username}:{self.server_id}"

        # For better debugging
        self.host = hostname  # Alias for compatibility
        self.operation_count = 0
        self.last_activity = datetime.now()

    @property
    def is_connected(self) -> bool:
        """Check if the client is connected and ready for operations

        Returns:
            bool: True if connected and ready, False otherwise
        """
        return self._connected and self._sftp_client is not None

    async def check_connection(self) -> bool:
        """Check if the connection is still valid by attempting a simple operation.

        Returns:
            bool: True if connection is valid, False otherwise
        """
        if not self._connected or not self._sftp_client:
            return False

        try:
            # Simple non-invasive check - try to get current directory
            async with asyncio.timeout(5.0):  # 5 second timeout for check
                try:
                    await self._sftp_client.getcwd()
                    self.last_activity = datetime.now()
                    return True
                except Exception:
                    pass

            # Try a secondary check by listing a directory
            async with asyncio.timeout(5.0):
                try:
                    await self._sftp_client.listdir(".")
                    self.last_activity = datetime.now()
                    return True
                except Exception:
                    logger.warning(f"Connection to {self.connection_id} appears stale, will reconnect")
                    await self.disconnect()
                    return False

        except asyncio.TimeoutError:
            logger.warning(f"Connection check to {self.connection_id} timed out")
            await self.disconnect()
            return False
        except Exception as e:
            logger.warning(f"Connection check failed: {e}")
            await self.disconnect()
            return False

    @retryable(max_retries=3, delay=1.0, backoff=2.0, 
               exceptions=(asyncio.TimeoutError, ConnectionError, OSError))
    async def connect(self) -> 'SFTPClient':
        """Connect to SFTP server with retries and exponential backoff

        This method will attempt to establish a connection up to max_retries times
        with an exponential backoff delay between attempts.

        Returns:
            self: The SFTPClient instance for method chaining
            
        Note:
            Check self.is_connected property to determine connection success.
            This method always returns self (the client object) for method chaining,
            not a boolean indicating success.
        """
        # Track memory usage before connection attempt
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            logger.debug(f"Memory before SFTP connection: {memory_mb:.2f}MB")
        except (ImportError, Exception):
            pass

        # If we already have a working connection, use it
        if self._connected and self._sftp_client and await self.check_connection():
            return self

        # Track details for diagnostic purposes
        start_time = datetime.now()
        self.last_operation = "connect"
        self.last_error = None
        self._connection_attempts += 1

        # Add jitter to prevent connection stampedes
        jitter = random.uniform(0, 0.5) * self._connection_attempts
        await asyncio.sleep(jitter)  # Actually use the jitter value

        # Prevent too many rapid connection attempts
        if self._connection_attempts > 5:
            logger.warning(f"Too many connection attempts for {self.connection_id}, throttling")
            # Reset and throttle to prevent resource exhaustion
            await asyncio.sleep(30)
            self._connection_attempts = 1

        # For cleanup in finally block
        temp_ssh_client = None
        
        try:
            # Check required credentials and validate hostname
            if not self.hostname or not self.username:
                logger.error(f"Missing required SFTP credentials for {self.server_id}")
                self._connected = False
                return self

            # Early exit for obviously invalid test/example hostnames
            if '.example.' in self.hostname or self.hostname == 'localhost':
                logger.warning(f"Invalid hostname detected for {self.server_id}: {self.hostname}")
                self._connected = False
                return self

            logger.info(f"Connecting to SFTP server: {self.hostname}:{self.port} (attempt {self._connection_attempts})")

            # Use asyncio timeout for more reliable timeouts
            async with asyncio.timeout(self.timeout):
                # Create asyncssh connection with improved settings
                temp_ssh_client = await asyncssh.connect(
                    host=self.hostname,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    known_hosts=None,  # Disable known hosts check
                    connect_timeout=self.timeout,
                    login_timeout=self.timeout,
                    keepalive_interval=30,   # Send keepalive every 30 seconds
                    keepalive_count_max=3    # Disconnect after 3 failed keepalives
                )
                
                # Only assign to instance var after successful connection
                self._ssh_client = temp_ssh_client
                temp_ssh_client = None  # Prevent double close in finally block

                # Get SFTP client
                self._sftp_client = await self._ssh_client.start_sftp_client()

            self._connected = True
            self._connection_attempts = 0
            self.last_activity = datetime.now()
            elapsed = (datetime.now() - start_time).total_seconds()

            logger.info(f"Connected to SFTP server: {self.connection_id} in {elapsed:.2f}s")
            
            # Test connection with simple operation
            try:
                await self._sftp_client.listdir('/')
                logger.debug(f"SFTP connection verified with test operation")
            except Exception as e:
                logger.warning(f"SFTP connection test failed, but connection established: {e}")
                # Continue despite test failure
            
            # Trigger garbage collection after connection
            try:
                import gc
                collected = gc.collect()
                logger.debug(f"GC after SFTP connection: {collected} objects collected")
            except Exception:
                pass
                
            return self

        except (asyncio.TimeoutError, ConnectionRefusedError, asyncssh.DisconnectError) as e:
            self._connected = False
            self.last_error = f"{type(e).__name__}: {str(e)}"

            # If authentication failed, don't retry to avoid account lockout
            if "Auth failed" in str(e) or "Permission denied" in str(e):
                logger.error(f"Authentication failed for {self.connection_id}, will not retry to avoid lockout")
                self._connection_attempts = self.max_retries + 1  # Exceed max retries to prevent further attempts
                return self

            logger.error(f"Failed to connect to SFTP server {self.connection_id}: {self.last_error}")

            # If we've exceeded max attempts, don't raise (stops retries) 
            if self._connection_attempts >= self.max_retries:
                return self

            # Otherwise propagate for retry
            raise

        except Exception as e:
            self._connected = False
            self.last_error = f"{type(e).__name__}: {str(e)}"

            # Log with full stack trace for debugging
            logger.error(f"Unexpected error connecting to SFTP server {self.connection_id}: {self.last_error}", 
                        exc_info=True)
            
            # If we've exceeded max attempts, don't raise (stops retries)
            if self._connection_attempts >= self.max_retries:
                return self
                
            # Otherwise propagate for retry
            raise
            
        finally:
            # Clean up temporary SSH client if it exists and wasn't assigned
            if temp_ssh_client:
                try:
                    temp_ssh_client.close()
                except Exception:
                    pass

    async def disconnect(self):
        """Disconnect from SFTP server and clean up resources"""
        # Clean up connection pool if this client is in it
        if hasattr(self, 'connection_id'):
            async with POOL_LOCK:
                if self.connection_id in CONNECTION_POOL:
                    if CONNECTION_POOL[self.connection_id] is self:
                        logger.info(f"Removing {self.connection_id} from connection pool")
                        CONNECTION_POOL.pop(self.connection_id, None)

        # Clean up active operations tracking
        if hasattr(self, 'server_id') and self.server_id:
            if self.server_id in ACTIVE_OPERATIONS:
                logger.info(f"Clearing {len(ACTIVE_OPERATIONS[self.server_id])} active operations for {self.server_id}")
                ACTIVE_OPERATIONS.pop(self.server_id, None)

        # Close SFTP client
        if self._sftp_client:
            try:
                # The SFTP client will be closed when the SSH client is closed
                # but we set it to None to avoid any further operations
                pass
            except Exception as e:
                logger.warning(f"Error handling SFTP client: {e}")
            self._sftp_client = None

        # Close SSH client
        if self._ssh_client:
            try:
                self._ssh_client.close()
            except Exception as e:
                logger.warning(f"Error closing SSH client: {e}")
            self._ssh_client = None

        self._connected = False
        logger.info(f"Disconnected from SFTP server: {self.connection_id}")

    @retryable(max_retries=1, delay=1.0, backoff=1.5, 
               exceptions=(asyncio.TimeoutError, ConnectionError, OSError))
    async def ensure_connected(self):
        """Ensure connection to SFTP server with proper error handling

        This will check the connection, reconnect if necessary, and retry on failure.
        Note: Reduced retries to prevent excessive resource consumption.
        """
        try:
            # Fast path - already connected
            if self._connected and self._sftp_client and self._ssh_client:
                # Check if connection is still valid (quick check)
                if await self.check_connection():
                    return

            # If we've had multiple failures recently, don't keep retrying
            # This prevents the bot from wasting resources on failing connections
            if self._connection_attempts > 3:
                logger.warning(f"Too many recent connection attempts for {self.connection_id}, backing off")
                # Don't raise an exception, just return and let the caller handle a null connection
                self.last_error = "Too many recent connection attempts, backing off"
                return

            # Need to connect or reconnect
            sftp = await self.connect()

            # Verify connection was successful
            if not sftp._connected:
                logger.warning(f"Failed to establish SFTP connection for {self.connection_id}")
                # Don't raise, just return and let caller handle missing connection
                return

        except (asyncio.TimeoutError, ConnectionRefusedError) as e:
            # If we get a connection timeout or refusal, don't keep retrying
            # This is likely a server issue that won't resolve quickly
            logger.error(f"Connection refused or timed out for {self.connection_id}: {e}")
            self.last_error = f"Connection refused: {str(e)}"
            # Don't raise to prevent needless retries

        except (ConnectionError, OSError) as e:
            # Other connection errors might be temporary
            logger.error(f"Connection error in ensure_connected: {e}")
            # Let the retry decorator handle this
            raise

        except Exception as e:
            logger.error(f"Unexpected error in ensure_connected: {e}")
            self.last_error = f"Connection error: {str(e)}"
            # Don't raise to prevent bot crashes on connection errors
            # The caller will handle the missing connection

    async def list_directory(self, directory: str) -> List[str]:
        """List files in directory

        Args:
            directory: Directory to list

        Returns:
            List of filenames
        """
        await self.ensure_connected()

        try:
            if self._sftp_client:
                entries = await self._sftp_client.listdir(directory)
                self.last_activity = datetime.now()  # Update last activity timestamp
                self.operation_count += 1
                return entries
            else:
                logger.error(f"SFTP client is missing when trying to list directory {directory}")
                return []
        except Exception as e:
            logger.error(f"Failed to list directory {directory}: {e}")
            return []

    async def get_file_info(self, path: str) -> Optional[Dict[str, Any]]:
        """Get file information

        Args:
            path: File path

        Returns:
            File information or None if not found
        """
        await self.ensure_connected()

        try:
            if self._sftp_client:
                stat = await self._sftp_client.stat(path)
                self.last_activity = datetime.now()  # Update last activity timestamp
                self.operation_count += 1

                # Create a stat-like object for compatibility with os.stat
                result = {
                    "size": stat.size,
                    "mtime": datetime.fromtimestamp(stat.mtime),
                    "atime": datetime.fromtimestamp(stat.atime),
                    "is_dir": stat.type == 2,  # asyncssh.FILEXFER_TYPE_DIRECTORY = 2
                    "is_file": stat.type == 1,  # asyncssh.FILEXFER_TYPE_REGULAR = 1
                    "permissions": stat.permissions,
                    # Add standard os.stat attributes for compatibility
                    "st_mtime": stat.mtime,
                    "st_size": stat.size,
                    "st_atime": stat.atime,
                    "st_mode": stat.permissions if hasattr(stat, 'permissions') else 0
                }
                return result
            else:
                logger.error(f"SFTP client is missing when trying to get file info for {path}")
                return None
        except Exception as e:
            logger.error(f"Failed to get file info for {path}: {e}")
            return None

    async def _download_to_memory(self, remote_path: str) -> Optional[bytes]:
        """Helper to download file to memory with multiple implementation strategies
        
        Args:
            remote_path: Path to file on SFTP server
            
        Returns:
            bytes: File contents or None if error
        """
        if not self._sftp_client:
            logger.error("Not connected when trying to download to memory")
            return None
            
        # Try multiple methods to download file to memory
        try:
            # Special case for AsyncSSH - detect by module
            # This check avoids attribute errors when the client doesn't have expected methods
            try:
                import asyncssh
                if isinstance(self._sftp_client, asyncssh.SFTPClient):
                    logger.info(f"Detected AsyncSSH SFTP client, using optimized methods")
                    try:
                        # Try the direct AsyncSSH method for reading files
                        content = await self._sftp_client.readfile(remote_path)
                        self.last_activity = datetime.now()
                        if isinstance(content, str):
                            content = content.encode('utf-8')
                        logger.info(f"Downloaded {remote_path} using AsyncSSH readfile ({len(content)} bytes)")
                        return content
                    except Exception as ssh_err:
                        logger.warning(f"AsyncSSH readfile failed: {ssh_err}, trying other methods")
            except (ImportError, Exception) as e:
                # If asyncssh isn't available or there's another error, continue with other methods
                logger.debug(f"AsyncSSH special handling skipped: {e}")
                
            # Method 1: Use getfo if available (most efficient)
            if hasattr(self._sftp_client, 'getfo'):
                try:
                    file_obj = io.BytesIO()
                    await self._sftp_client.getfo(remote_path, file_obj)
                    self.last_activity = datetime.now()
                    file_obj.seek(0)
                    content = file_obj.read()
                    logger.info(f"Downloaded {remote_path} to memory using getfo ({len(content)} bytes)")
                    return content
                except Exception as getfo_err:
                    logger.warning(f"getfo method failed: {getfo_err}, trying alternatives")
            
            # Method 2: Use read if available
            if hasattr(self._sftp_client, 'read'):
                try:
                    content = await self._sftp_client.read(remote_path)
                    self.last_activity = datetime.now()
                    logger.info(f"Downloaded {remote_path} to memory using read ({len(content)} bytes)")
                    return content
                except Exception as read_err:
                    logger.warning(f"read method failed: {read_err}, trying alternatives")
                    
            # Method 3: Use open+read if available
            if hasattr(self._sftp_client, 'open'):
                try:
                    async with self._sftp_client.open(remote_path, 'rb') as f:
                        content = await f.read()
                    self.last_activity = datetime.now()
                    logger.info(f"Downloaded {remote_path} to memory using open+read ({len(content)} bytes)")
                    return content
                except Exception as open_err:
                    logger.warning(f"open method failed: {open_err}, trying alternatives")
            
            # Method 4: Try other AsyncSSH-style methods by string matching on type
            try:
                # Check if this looks like an asyncssh SFTP client
                client_type = str(type(self._sftp_client)).lower()
                if 'asyncssh' in client_type or 'sftp' in client_type:
                    # Try some common methods that might be available
                    for method_name in ['readfile', 'getfile', 'download', 'read_file', 'fetch']:
                        if hasattr(self._sftp_client, method_name):
                            try:
                                logger.info(f"Trying alternative method: {method_name}")
                                
                                if method_name in ['readfile', 'read_file', 'fetch', 'download']:
                                    # Methods that return content directly
                                    content = await getattr(self._sftp_client, method_name)(remote_path)
                                    if content:
                                        self.last_activity = datetime.now()
                                        if isinstance(content, str):
                                            content = content.encode('utf-8') 
                                        logger.info(f"Downloaded using {method_name} ({len(content)} bytes)")
                                        return content
                                else:
                                    # Methods that write to a file-like object
                                    file_obj = io.BytesIO()
                                    await getattr(self._sftp_client, method_name)(remote_path, file_obj)
                                    file_obj.seek(0)
                                    content = file_obj.read()
                                    if content:
                                        self.last_activity = datetime.now()
                                        logger.info(f"Downloaded using {method_name} ({len(content)} bytes)")
                                        return content
                            except Exception as method_err:
                                logger.debug(f"Method {method_name} failed: {method_err}")
            except Exception as type_err:
                logger.debug(f"Type-based method detection failed: {type_err}")
                    
            # Method 5: Use get with a temporary file (least efficient but reliable fallback)
            temp_file = f"temp_{int(time.time())}.tmp"
            try:
                await self._sftp_client.get(remote_path, temp_file)
                self.last_activity = datetime.now()
                
                # Read the temp file
                with open(temp_file, 'rb') as f:
                    content = f.read()
                
                # Clean up temp file
                try:
                    os.remove(temp_file)
                except Exception as rm_err:
                    logger.warning(f"Failed to remove temp file {temp_file}: {rm_err}")
                    
                logger.info(f"Downloaded {remote_path} to memory using temp file ({len(content)} bytes)")
                return content
            except Exception as temp_err:
                logger.warning(f"Temp file method failed: {temp_err}")
                try:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                except:
                    pass
            
            # If we reach here, all methods failed
            logger.error(f"All download methods failed for {remote_path}")
            return None
            
        except Exception as e:
            logger.error(f"Error downloading file {remote_path} to memory: {e}")
            return None
            
    async def download_file(self, remote_path: str, local_path: Optional[str] = None) -> Optional[bytes]:
        """Download file from SFTP server

        Args:
            remote_path: Remote file path
            local_path: Optional local file path to save to

        Returns:
            File contents as bytes if local_path is not provided, otherwise None
        """
        await self.ensure_connected()

        try:
            if not self._sftp_client:
                logger.error(f"SFTP client is missing when trying to download file {remote_path}")
                return None

            # Update last activity timestamp before operation
            self.last_activity = datetime.now()
            self.operation_count += 1

            if local_path:
                # Download to file - try multiple methods
                try:
                    await self._sftp_client.get(remote_path, local_path)
                    # Update again after successful operation
                    self.last_activity = datetime.now()
                    logger.info(f"Downloaded {remote_path} to {local_path}")
                    return None
                except Exception as get_err:
                    logger.warning(f"Error using get method: {get_err}, trying alternative methods")
                    
                    # Try to download to memory first, then write to file
                    try:
                        content = await self._download_to_memory(remote_path)
                        if content:
                            with open(local_path, 'wb') as f:
                                f.write(content)
                            logger.info(f"Downloaded {remote_path} to {local_path} (memory+write method)")
                            return None
                    except Exception as alt_err:
                        logger.error(f"Failed to download with alternative method: {alt_err}")
                        return None
            else:
                # Download to memory
                return await self._download_to_memory(remote_path)

        except Exception as e:
            logger.error(f"Failed to download file {remote_path}: {e}")
            return None

    async def read_file_by_chunks(self, remote_path: str, chunk_size: int = 4096) -> Optional[List[bytes]]:
        """Read file by chunks

        Args:
            remote_path: Remote file path
            chunk_size: Chunk size in bytes

        Returns:
            List of chunks or None if failed
        """
        await self.ensure_connected()

        try:
            if not self._sftp_client:
                logger.error(f"SFTP client is missing when trying to read file {remote_path} by chunks")
                return None

            chunks = []
            async with self._sftp_client.open(remote_path, 'rb') as f:
                while True:
                    chunk = await f.read(chunk_size)
                    if not chunk:
                        break
                    chunks.append(chunk)

            logger.info(f"Read {remote_path} by chunks ({len(chunks)} chunks)")
            return chunks

        except Exception as e:
            logger.error(f"Failed to read file {remote_path} by chunks: {e}")
            return None

    async def read_file(self, remote_path: str, start_line: int = 0, max_lines: int = -1) -> Optional[List[str]]:
        """Read file from remote server with line control

        This method is specifically designed for the historical parse functionality,
        allowing reading a specific number of lines starting from a given position.

        Args:
            remote_path: Remote file path
            start_line: Line number to start reading from (0-indexed)
            max_lines: Maximum number of lines to read (-1 for all lines)

        Returns:
            List of text lines or None on error
        """
        await self.ensure_connected()

        try:
            # Download file content to memory
            content_data = await self.download_file(remote_path)
            if not content_data:
                logger.error(f"Could not download file content for {remote_path}")
                return None

            # Split into lines and apply start/max limits
            all_lines = content_data.decode('utf-8', errors='replace').splitlines()

            # Calculate end based on start and max_lines
            end_line = len(all_lines) if max_lines < 0 else min(start_line + max_lines, len(all_lines))

            # Get the requested lines
            requested_lines = all_lines[start_line:end_line]

            logger.debug(f"Read {len(requested_lines)} lines from {remote_path} (start={start_line}, max={max_lines})")
            return requested_lines

        except Exception as e:
            logger.error(f"Failed to read file {remote_path}: {e}")
            traceback.print_exc()
            return None

    @with_operation_tracking("find_files")
    async def list_files(self, directory: str, pattern: str = r".*\.csv") -> List[str]:
        """List files in directory matching pattern (alias for find_files_by_pattern)

        This is a compatibility method to match SFTPManager's interface.

        Args:
            directory: Directory to list files from
            pattern: Regex pattern to match files against

        Returns:
            List[str]: List of matching file paths
        """
        return await self.find_files_by_pattern(directory, pattern)

    @with_operation_tracking("find_files")
    async def find_files_by_pattern(self, directory: str, pattern: str, recursive: bool = True, max_depth: int = 10) -> List[str]:
        """Find files by pattern

        Enhanced to properly search through directory structures with map subdirectories.
        Specifically for Tower of Temptation structure /hostname_serverid/actual1/deathlogs/world_X/*.csv
        - Set default max_depth to 10 to ensure allmap directories are searched
        - Set default recursive to True to ensure proper traversal

        Args:
            directory: Directory to search
            pattern: Regular expression pattern for filenames
            recursive: Whether to search recursively (default: True)
            max_depth: Maximum recursion depth (default: 10)

        Returns:
            List of matching file paths (empty list if error occurs)
        """
        await self.ensure_connected()

        # Check if we're connectedafter trying to ensure connection
        if not self._connected or not self._sftp_client:
            logger.warning(f"Not connected to SFTP server when trying to find files in {directory}")
            return []

        try:
            # Explicitly initialize result list to ensure it's never None
            result = []
            pattern_re = re.compile(pattern)

            # Pass the initialized result list to the recursive function
            await self._find_files_recursive(directory, pattern_re, result, recursive, max_depth, 0)

            # Return the populated result list
            return result
        except Exception as e:
            logger.error(f"Error in find_files_by_pattern: {str(e)}")
            return []

    @with_operation_tracking("find_files_recursive")
    async def find_files_recursive(self, directory: str, pattern: str, result: List[str], recursive: bool = True, max_depth: int = 10) -> None:
        """Public interface for recursive file search

        This method provides a public interface to the private _find_files_recursive method,
        allowing other components to use the recursive search functionality directly.

        Enhanced for Tower of Temptation directory structures to properly traverse map 
        subdirectories within deathlogs:
        /hostname_serverid/actual1/deathlogs/world_0/*.csv

        Args:
            directory: Directory to search
            pattern: Regular expression pattern string (will be compiled)
            result: List to store results in (modified in place)
            recursive: Whether to search recursively (default: True)
            max_depth: Maximum recursion depth (default: 10)
        """
        await self.ensure_connected()

        # Check if we're connected after trying to ensure connection
        if not self._connected or not self._sftp_client:
            logger.warning(f"Not connected to SFTP server when trying to find files recursively in {directory}")
            return

        try:
            # Compile pattern string to regex pattern
            pattern_re = re.compile(pattern)
            # Start recursive search from depth 0
            await self._find_files_recursive(directory, pattern_re, result, recursive, max_depth, 0)
        except Exception as e:
            logger.error(f"Error in find_files_recursive: {str(e)}")
            # Don't return anything - result is modified in place

    async def exists(self, path: str) -> bool:
        """Check if a path exists on the SFTP server

        Args:
            path: Path to check

        Returns:
            bool: True if path exists, False otherwise
        """
        await self.ensure_connected()

        if not self._connected or not self._sftp_client:
            logger.warning(f"Not connected to SFTP server when checking if path exists: {path}")
            self._connected = False
        return self

        try:
            # Use stat to check if path exists
            await self._sftp_client.stat(path)
            return True
        except Exception as e:
            logger.debug(f"Path does not exist or error accessing {path}: {str(e)}")
            return False

    async def is_file(self, path: str) -> bool:
        """Check if a path is a file (not a directory) on the SFTP server

        Args:
            path: Path to check

        Returns:
            bool: True if path is a file, False otherwise
        """
        await self.ensure_connected()

        if not self._connected or not self._sftp_client:
            logger.warning(f"Not connected to SFTP server when checking if path is a file: {path}")
            return False

        try:
            # First check if path exists
            if not await self.exists(path):
                return False

            # Use lstat to get file info
            stat_result = await self._sftp_client.lstat(path)
            # Check if it's a regular file (not a directory)
            return stat_result.type == 1  # 1 is for regular files in SFTP
        except Exception as e:
            logger.debug(f"Error checking if path is a file {path}: {str(e)}")
            return False

    async def get_log_file(self, server_dir: Optional[str] = None, base_path: Optional[str] = None) -> Optional[str]:
        """Get the Deadside.log file path
        
        Args:
            server_dir: Optional pre-constructed server directory (e.g., "hostname_serverid")
            base_path: Optional base path override for finding the log file

        Returns:
            Optional[str]: Path to Deadside.log if found, None otherwise
        """
        await self.ensure_connected()

        try:
            # CRITICAL: Always use original_server_id (numeric ID) for path construction 
            # This ensures consistent path construction with the CSV file functions
            path_server_id = self.original_server_id if hasattr(self, 'original_server_id') and self.original_server_id else self.server_id
            
            # Ensure we're using a string
            if path_server_id is not None:
                path_server_id = str(path_server_id)
            
            # Use the server_identity module for consistent path construction
            from utils.server_identity import identify_server
            
            # Get consistent numeric ID for path construction
            numeric_id, is_known = identify_server(
                server_id=path_server_id,
                hostname=self.hostname,
                server_name=getattr(self, 'server_name', None),
                guild_id=getattr(self, 'guild_id', None)
            )
            
            # Use the identified numeric ID
            if is_known or numeric_id != path_server_id:
                logger.info(f"Using identified numeric ID '{numeric_id}' for path construction instead of '{path_server_id}'")
                path_server_id = numeric_id
            
            # Log which server ID we're using for path construction
            logger.info(f"Using server ID '{path_server_id}' for path construction in get_log_file")

            # Use provided server_dir if available, otherwise construct it
            if not server_dir:
                hostname = self.hostname.split(':')[0] if self.hostname else "server" 
                server_dir = f"{hostname}_{path_server_id}"
                logger.info(f"Constructed server directory: {server_dir}")
            else:
                logger.info(f"Using provided server directory: {server_dir}")
            
            # Use provided base_path if available, otherwise construct it
            if not base_path:
                # For Deadside logs in Tower of Temptation's server structure
                # The path is always /hostname_serverid/Logs/Deadside.log where serverid is the numeric ID
                base_path = os.path.join("/", server_dir, "Logs")
                logger.info(f"Constructed log path: {base_path}")
            else:
                logger.info(f"Using provided log path: {base_path}")
                
            logger.info(f"Final path construction: server_dir={server_dir}, base_path={base_path}, server_id={path_server_id}")
            deadside_log = os.path.join(base_path, "Deadside.log")

            # Log the exact path we're checking
            logger.info(f"Looking for Deadside.log at path: {deadside_log}")

            # Verify file exists and log the path we're checking
            try:
                logger.info(f"Checking for Deadside.log at path: {deadside_log}")
                if not self._sftp_client:
                    logger.error(f"SFTP client is missing when trying to check for Deadside.log")
                    return None

                await self._sftp_client.stat(deadside_log)
                logger.info(f"Found Deadside.log at: {deadside_log}")
                return deadside_log
            except Exception as e:
                logger.warning(f"Deadside.log not found in {base_path}: {e}")
                return None

        except Exception as e:
            logger.error(f"Failed to get log file: {e}")
            return None

    async def _find_files_recursive(self, directory: str, pattern_re: re.Pattern, result: List[str], recursive: bool, max_depth: int, current_depth: int) -> List[str]:
        """Recursively find files by pattern with strict downward-only traversal

        Args:
            directory: Directory to search (must be under server root)
            pattern_re: Compiled regular expression pattern
            result: List to add results to 
            recursive: Whether to search recursively
            max_depth: Maximum recursion depth
            current_depth: Current recursion depth
            
        Returns:
            List[str]: List of found files
        """
        # Initialize result list if not provided
        if not isinstance(result, list):
            result = []

        # Clean and normalize directory path
        directory = os.path.normpath(directory)
        if not directory.startswith('/'):
            directory = os.path.join("/", directory)

        # Get server directory based on hostname/server_id
        hostname = self.hostname.split(':')[0] if self.hostname else "server"
        server_id = self.original_server_id if hasattr(self, 'original_server_id') and self.original_server_id else self.server_id
        server_dir = f"{hostname}_{server_id}"
        
        # Early validation of directory path
        if '..' in directory or '~' in directory:
            logger.warning(f"Rejecting invalid directory path: {directory}")
            return result

        if current_depth > max_depth:
            logger.debug(f"Max depth {max_depth} reached, stopping at directory: {directory}")
            return result

        # Normalize path for security
        directory = os.path.normpath(directory)
        
        # Build expected server directory path
        expected_server_path = os.path.join("/", server_dir)
        
        # Allow access to server directory and its subdirectories
        if not (directory == "/" or directory.startswith(expected_server_path)):
            logger.debug(f"Path {directory} not under server directory {expected_server_path}, skipping")
            return result

        # Early validation of directory path
        if '..' in directory or '~' in directory:
            logger.warning(f"Rejecting invalid directory path: {directory}")
            return result

        if current_depth > max_depth:
            logger.debug(f"Max depth {max_depth} reached at {directory}")
            return result

        # Safety check - make sure result is a valid list
        if result is None or not isinstance(result, list):
            logger.warning("Result list was None or not a list in _find_files_recursive, creating new list")
            result = []

        try:
            # Normalize directory paths with trailing slashes to ensure consistent path joining
            if not directory.endswith('/'):
                directory = directory + '/'

            # Detailed logging to trace exactly where the search is occurring
            if current_depth <= 1:  # Only log at top levels to avoid log spam
                logger.info(f"Scanning directory: {directory} (depth {current_depth}/{max_depth})")
            else:
                logger.debug(f"Scanning directory: {directory} (depth {current_depth}/{max_depth})")

            # Verify connection before proceeding
            if not self._connected or not self._sftp_client:
                logger.error(f"Not connected to SFTP server when trying to list directory {directory}")
                # Try to reconnect
                if await self.ensure_connected():
                    logger.info(f"Reconnected during recursive search in {directory}")
                else:
                    logger.error(f"Failed to reconnect during recursive search in {directory}")
                    return result

            # List directory contents with retry logic
            max_attempts = 2
            entries = []

            for attempt in range(1, max_attempts + 1):
                try:
                    entries = await self._sftp_client.listdir(directory)
                    if entries:
                        logger.debug(f"Found {len(entries)} entries in {directory}")
                        break
                    elif attempt < max_attempts:
                        logger.warning(f"No entries found in {directory}, retry attempt {attempt}/{max_attempts}")
                        await asyncio.sleep(0.5)  # Brief delay before retry
                except Exception as list_err:
                    logger.warning(f"Failed to list directory {directory} (attempt {attempt}/{max_attempts}): {list_err}")
                    if attempt < max_attempts:
                        # Try to reconnect before retrying
                        await self.ensure_connected()
                        await asyncio.sleep(0.5)  # Brief delay before retry
                    else:
                        logger.error(f"All attempts to list directory {directory} failed")
                        return result

            # Check if we ultimately found any entries
            if not entries:
                logger.debug(f"No entries found in directory {directory} after all attempts")
                return result

            # Process all entries
            for entry in entries:
                # Avoid . and .. entries that could cause loops
                if entry in ('.', '..'):
                    continue

                # Build full path properly
                entry_path = f"{directory}{entry}"

                try:
                    # Get file info with proper error handling
                    entry_info = await self.get_file_info(entry_path)

                    if not entry_info:
                        logger.warning(f"Could not get info for: {entry_path}")
                        continue

                    # Check if it's a CSV file
                    if entry_info["is_file"] and pattern_re.search(entry):
                        logger.debug(f"Found matching file: {entry_path}")
                        result.append(entry_path)

                    # Recursively explore directories if needed
                    elif entry_info["is_dir"] and recursive:
                        logger.debug(f"Exploring subdirectory: {entry_path}")
                        await self._find_files_recursive(entry_path, pattern_re, result, recursive, max_depth, current_depth + 1)

                except Exception as e:
                    logger.warning(f"Error processing entry {entry_path}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Failed to process directory {directory}: {e}")
            logger.debug(f"Directory error details:\n{traceback.format_exc()}")
            
        # Make sure we always return the result list
        return result

    @with_operation_tracking("find_csv")
    @retryable(max_retries=2, delay=1.0, backoff=1.5, 
               exceptions=(asyncio.TimeoutError, ConnectionError, OSError))
    async def find_csv_files(
        self, 
        directory: str, 
        date_range: Optional[Tuple[datetime, datetime]] = None,
        recursive: bool = True,
        max_depth: int = 10,  # Increased max_depth to handle map subdirectories
        include_hourly: bool = True,
        timeout: Optional[float] = 60.0,
        no_parent_search: bool = True
    ) -> List[str]:
        """Find CSV files in directory with enhanced error handling and date filtering

        Enhanced for Tower of Temptation directory structure where CSV files are in map subdirectories:
        /hostname_serverid/actual1/deathlogs/world_0/*.csv
        /hostname_serverid/actual1/deathlogs/world_1/*.csv

        This method finds CSV files in the specified directory, with support for:
        - Date range filtering based on filename patterns
        - Recursive directory traversal with depth limiting (max_depth increased to 10)
        - Operation timeout to prevent hanging
        - Automatic retries on connection errors
        - Detailed error reporting
        - Automatic path correction to find deathlogs directory

        Args:
            directory: Directory to search (will be adjusted to deathlogs path if needed)
            date_range: Optional tuple of (start_date, end_date) to filter by filename date
            recursive: Whether to search recursively (default: True)
            max_depth: Maximum recursion depth (default: 10 to reach map subdirectories)
            include_hourly: Whether to include hourly CSV files (those with HH.MM.SS in name)
            timeout: Operation timeout in seconds (None for no timeout)
            no_parent_search: If True, never search parent directories

        Returns:
            List of CSV file paths sorted by date (newest first if date_range is provided)
        """
        start_time = datetime.now()
        self.last_operation = f"find_csv_files({directory})"
        self.last_error = None
        self.operation_count += 1

        # Normalize directory path
        if not directory or directory == ".":
            directory = await self._get_current_directory()

        logger.info(f"Searching for CSV files in {directory} (recursive={recursive}, max_depth={max_depth}, no_parent_search={no_parent_search})")

        # Prevent parent directory traversal if requested
        if no_parent_search and '..' in directory:
            logger.warning(f"Parent directory search attempted but blocked: {directory}")
            return []

        try:
            # Use timeout to prevent hanging
            if timeout:
                async with asyncio.timeout(timeout):
                    return await self._find_csv_files_impl(
                        directory, date_range, recursive, max_depth, include_hourly, no_parent_search
                    )
            else:
                return await self._find_csv_files_impl(
                    directory, date_range, recursive, max_depth, include_hourly, no_parent_search
                )

        except asyncio.TimeoutError:
            elapsed = (datetime.now() - start_time).total_seconds()
            self.last_error = f"Operation timed out after {elapsed:.1f}s"
            logger.error(f"CSV file search in {directory} timed out after {elapsed:.1f}s")
            raise

        except Exception as e:
            elapsed = (datetime.now() - start_time).total_seconds()
            self.last_error = f"{type(e).__name__}: {str(e)}"
            logger.error(f"Error searching for CSV files in {directory}: {e} (after {elapsed:.1f}s)")

            # Convert to retryable error types
            if isinstance(e, (OSError, IOError, ConnectionError, asyncio.TimeoutError)):
                raise  # Let retry decorator handle these types directly
            else:
                # Wrap in ConnectionError for retry handling
                raise ConnectionError(f"SFTP operation failed: {str(e)}")

    async def _get_current_directory(self) -> str:
        """Get current directory on SFTP server"""
        await self.ensure_connected()
        try:
            if not self._sftp_client:
                logger.error("SFTP client is missing when trying to get current directory")
                return "."

            return await self._sftp_client.getcwd() or "."
        except Exception as e:
            logger.warning(f"Failed to get current directory: {e}")
            return "."

    async def _find_csv_files_impl(
        self,
        directory: str,
        date_range: Optional[Tuple[datetime, datetime]] = None,
        recursive: bool = True,
        max_depth: int = 10,  # Increased max depth to ensure we can reach map directories (world_0, etc.)
        include_hourly: bool = True,
        no_parent_search: bool = True
    ) -> List[str]:
        """Implementation of CSV file search with date filtering, enhanced for Tower of Temptation files

        This implementation has been specifically enhanced to handle the Emeralds Killfeed
        directory structure where CSV files are located in map subdirectories:
        /hostname_serverid/actual1/deathlogs/world_0/*.csv
        /hostname_serverid/actual1/deathlogs/world_1/*.csv
        etc.
        """
        # Ensure we're connected before searching
        await self.ensure_connected()

        logger.debug(f"Starting CSV file search in directory: {directory} (recursive={recursive}, max_depth={max_depth}, no_parent_search={no_parent_search})")

        # CRITICAL: Always use original_server_id (numeric ID) for path construction
        # This ensures consistent path construction across functions
        hostname = self.hostname.split(':')[0] if self.hostname else "server"
        
        # Check for original_server_id and use it for path construction
        path_server_id = self.original_server_id if hasattr(self, 'original_server_id') and self.original_server_id else self.server_id
        
        # Ensure we're using a string
        if path_server_id is not None:
            path_server_id = str(path_server_id)
        
        # Log which server ID we're using for path construction
        logger.info(f"Using server ID '{path_server_id}' for CSV path construction")
        
        server_path = f"{hostname}_{path_server_id}"

        # Enforce the canonical path for CSV files - always use /hostname_serverid/actual1/deathlogs
        # For Emeralds Killfeed server structure, CSV files are in subdirectories under:
        # /hostname_serverid/actual1/deathlogs/**/*.csv
        canonical_path = os.path.join("/", server_path, "actual1", "deathlogs")
        directory = os.path.normpath(canonical_path)
        
        # Always set recursive=True for CSV files since we need to search subdirectories
        recursive = True
        
        # Increase max_depth to ensure we can find files in nested map directories
        max_depth = max(max_depth, 10)
        
        # Log the standardized path we're using
        logger.info(f"Using standardized CSV path: {directory}")

        # Validate that we're in the correct directory structure
        if not directory.startswith(os.path.join("/", server_path)):
            logger.warning(f"Attempted to search outside server directory: {directory}")
            return []

        # Prevent parent directory traversal
        if '..' in directory:
            logger.warning(f"Parent directory traversal attempted: {directory}")
            return []

        logger.info(f"Using standardized path: {directory}")

        # Check if we need to look for map subdirectories first
        known_map_dirs = ["world_0", "world0", "world_1", "world1", "map_0", "map0", "main", "default"]
        map_directories = []

        # If we're in the deathlogs directory, check for map subdirectories
        if "/deathlogs" in directory and await self.exists(directory):
            logger.debug(f"Checking for map subdirectories in {directory}")

            # First try to list the directory contents to find map subdirectories
            try:
                entries = await self._sftp_client.listdir(directory)
                logger.debug(f"Found {len(entries)} entries in directory")

                # Check each entry to see if it's a directory
                for entry in entries:
                    if entry in (".", ".."):
                        continue

                    entry_path = os.path.join(directory, entry)
                    try:
                        # Check if it's a directory
                        is_dir = False
                        try:
                            stat_info = await self._sftp_client.stat(entry_path)
                            is_dir = stat.S_ISDIR(stat_info.st_mode)
                        except:
                            # If stat fails, try get_file_info which has better error handling
                            file_info = await self.get_file_info(entry_path)
                            is_dir = file_info and file_info.get("is_dir", False)

                        if is_dir:
                            # Check if entry is one of our known map directory names or contains "world" or "map"
                            if entry in known_map_dirs or "world" in entry.lower() or "map" in entry.lower():
                                logger.debug(f"Found map directory: {entry_path}")
                                map_directories.append(entry_path)
                    except Exception as e:
                        logger.debug(f"Error checking if {entry_path} is a directory: {e}")
            except Exception as e:
                logger.warning(f"Error listing directory {directory}: {e}")

            if map_directories:
                logger.debug(f"Found {len(map_directories)} map directories")

        # Define patterns for matching Emeralds Killfeed CSV files (both pre and post April formats)
        csv_patterns = [
            # Primary pattern - matches YYYY.MM.DD-HH.MM.SS.csv (Emeralds Killfeed standard)
            r'\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}\.csv$',

            # Alternative patterns - match various date formats
            r'\d{4}\.\d{2}\.\d{2}.*\.csv$',    # YYYY.MM.DD*.csv (any time format)
            r'\d{4}-\d{2}-\d{2}.*\.csv$',      # YYYY-MM-DD*.csv (ISO date format)

            # Generic CSV fallback as last resort
            r'.*\.csv$'
        ]

        all_csv_files = []

        # If we found map directories, search each one first - this is the most efficient approach
        if map_directories:
            logger.debug(f"Searching in {len(map_directories)} map directories")

            for map_dir in map_directories:
                # Only use the primary pattern first for efficiency
                try:
                    map_files = await self.find_files_by_pattern(
                        map_dir,
                        csv_patterns[0],  # Primary date pattern
                        recursive=False,  # Don't need recursion within map directory
                        max_depth=1       # Just search the map directory itself
                    )

                    if map_files:
                        logger.debug(f"Found {len(map_files)} files in map directory")
                        all_csv_files.extend(map_files)
                        continue  # Go to next map directory since we found files

                    # Try fallback pattern only if needed
                    map_files = await self.find_files_by_pattern(
                        map_dir,
                        r'\.csv$',  # Generic .csv fallback
                        recursive=False,
                        max_depth=1
                    )

                    if map_files:
                        logger.debug(f"Found {len(map_files)} generic CSV files in map directory")
                        all_csv_files.extend(map_files)
                except Exception as e:
                    logger.warning(f"Error searching map directory: {e}")

            # If we found CSV files in map directories, use those
            if all_csv_files:
                logger.debug(f"Found {len(all_csv_files)} total CSV files in map directories")
                # Only log a sample in debug mode
                if logger.level <= logging.DEBUG and all_csv_files:
                    sample = all_csv_files[:3] if len(all_csv_files) > 3 else all_csv_files
                    logger.debug(f"Sample files: {sample}")
                csv_files = all_csv_files
                return csv_files  # Return files found in map directories - early return for efficiency

        # If we didn't find map directories or files in them, try with date-specific pattern across entire directory
        logger.debug(f"Searching with date-formatted pattern across directory")
        date_pattern_files = await self.find_files_by_pattern(
            directory, 
            csv_patterns[0],  # Use the primary pattern for Emeralds Killfeed
            recursive=recursive, 
            max_depth=max_depth
        )

        # If found files with date pattern, use those
        if date_pattern_files:
            logger.debug(f"Found {len(date_pattern_files)} date-formatted CSV files")
            # Only log sample in debug mode
            if logger.level <= logging.DEBUG and date_pattern_files:
                sample = date_pattern_files[:3] if len(date_pattern_files) > 3 else date_pattern_files
                logger.debug(f"Sample files: {sample}")
            csv_files = date_pattern_files
        else:
            # Fall back to general CSV pattern
            logger.debug(f"No date-formatted CSV files found, falling back to general CSV pattern")
            csv_files = await self.find_files_by_pattern(directory, r'\.csv$', recursive, max_depth)
            logger.debug(f"Found {len(csv_files)} total CSV files")
            # Only log sample in debug mode
            if logger.level <= logging.DEBUG and csv_files:
                sample = csv_files[:3] if len(csv_files) > 3 else csv_files
                logger.debug(f"Sample files: {sample}")

        # Return early if no files found
        if not csv_files:
            # Try multiple fallback approaches - look specifically in common subdirectories
            # Expanded list of possible locations
            # Standard fallback directories
            fallback_dirs = [
                os.path.join(directory, "deathlogs"),
                os.path.join(directory, "logs"),
                os.path.join(directory, "Logs"),
                os.path.join(directory, "LOG"),
                os.path.join(directory, "actual1", "deathlogs"),
                os.path.join(directory, "actual1", "logs"),
                os.path.join(directory, "actual1", "Logs"),
                os.path.join(directory, "actual", "deathlogs"),
                os.path.join(directory, "actual", "logs"),
                os.path.join(directory, "..", "logs"),
                os.path.join(directory, "..", "deathlogs"),
                os.path.join("/", "logs", os.path.basename(directory)),
                os.path.join("/", "deathlogs", os.path.basename(directory)),
                os.path.join("/", "Logs", os.path.basename(directory)),
                # Additional Emeralds Killfeed specific paths
                os.path.join("/", "data", "logs"),
                os.path.join("/", "data", "csv"),
                os.path.join("/", "data", "deathlogs"),
                os.path.join("/", "game", "logs"),
                os.path.join("/", "game", "deathlogs")
            ]

            # CRITICAL FIX: Add specific map subdirectory patterns for Emeralds Killfeed
            # Each map is in its own subdirectory under the deathlogs path
            deathlogs_dirs = [
                os.path.join(directory, "deathlogs"),
                os.path.join(directory, "actual1", "deathlogs"),
                os.path.join(directory, "actual", "deathlogs")
            ]

            # Attempt to discover map subdirectories under deathlogs paths
            map_subdirs = []
            for deathlog_dir in deathlogs_dirs:
                try:
                    if await self.exists(deathlog_dir):
                        logger.debug(f"Checking for map subdirectories in: {deathlog_dir}")
                        try:
                            # List all subdirectories (these would be the map directories)
                            dirs = await self._sftp_client.listdir(deathlog_dir)
                            for subdir in dirs:
                                if not subdir.startswith("."):  # Skip hidden directories
                                    map_path = os.path.join(deathlog_dir, subdir)
                                    try:
                                        # Check if it's a directory using file_info
                                        file_info = await self.get_file_info(map_path)
                                        if file_info and file_info.get("is_dir", False):
                                            map_subdirs.append(map_path)
                                            logger.debug(f"Found map subdirectory: {map_path}")
                                    except Exception as subdir_err:
                                        logger.debug(f"Error checking map subdirectory {map_path}: {subdir_err}")
                        except Exception as e:
                            logger.warning(f"Error listing map subdirectories in {deathlog_dir}: {e}")
                except Exception as e:
                    logger.warning(f"Error checking for deathlogs directory {deathlog_dir}: {e}")

            # Add map subdirectories to the fallback directories
            # These take priority since they're the most likely to contain CSV files
            fallback_dirs = map_subdirs + fallback_dirs
            logger.debug(f"Added {len(map_subdirs)} map subdirectories to search paths")

            logger.debug(f"No files found in primary search, trying {len(fallback_dirs)} fallback directories")

            for fallback_dir in fallback_dirs:
                logger.debug(f"Trying fallback directory: {fallback_dir}")
                try:
                    if await self.exists(fallback_dir):
                        logger.debug(f"Directory exists, searching for CSV files")
                        # First try with date pattern
                        fallback_files = await self.find_files_by_pattern(
                            fallback_dir, r'\d{4}[.-]\d{2}[.-]\d{2}.*\.csv$', recursive=True, max_depth=10
                        )

                        # If no files with date pattern, try general CSV pattern
                        if not fallback_files:
                            logger.debug(f"No date-formatted files, trying general CSV pattern")
                            fallback_files = await self.find_files_by_pattern(
                                fallback_dir, r'\.csv$', recursive=True, max_depth=10
                            )

                        if fallback_files:
                            logger.debug(f"Found {len(fallback_files)} CSV files in fallback directory")
                            # Only log sample in debug mode
                            if logger.level <= logging.DEBUG and fallback_files:
                                sample = fallback_files[:3] if len(fallback_files) > 3 else fallback_files
                                logger.debug(f"Sample files: {sample}")
                            csv_files = fallback_files
                            break
                        else:
                            logger.debug(f"No CSV files found in this directory")
                    else:
                        logger.debug(f"Fallback directory does not exist")
                except Exception as e:
                    logger.warning(f"Error checking fallback directory: {e}")
                    continue

            # If still no files found
            if not csv_files:
                logger.warning(f"No CSV files found in {directory} or any fallback directories")
                return []

        # Return all files if no date range
        if not date_range:
            return sorted(csv_files)

        # Parse start and end dates
        start_date, end_date = date_range
        filtered_files = []
        file_dates = {}  # Store dates for sorting

        # Date format patterns to try
        date_patterns = [
            # Standard date formats
            (r'(\d{4}[.-]\d{2}[.-]\d{2})', ['%Y-%m-%d', '%Y.%m.%d']),

            # Date with time formats
            (r'(\d{4}[.-]\d{2}[.-]\d{2}[.-]\d{2}[.-]\d{2}[.-]\d{2})', ['%Y-%m-%d-%H-%M-%S', '%Y.%m.%d.%H.%M.%S']),
            (r'(\d{4}[.-]\d{2}[.-]\d{2})[^0-9](\d{2})[^0-9](\d{2})[^0-9](\d{2})', 
             ['combined:%Y-%m-%d %H:%M:%S', 'combined:%Y.%m.%d %H:%M:%S']),

            # Fallback pattern with just year-month
            (r'(\d{4}[.-]\d{2})', ['%Y-%m', '%Y.%m'])
        ]

        for file_path in csv_files:
            # Extract date from filename using patterns
            file_name = os.path.basename(file_path)
            file_date = None
            matched = False

            # Try all patterns until one matches
            for pattern, formats in date_patterns:
                date_match = re.search(pattern, file_name)
                if not date_match:
                    continue

                # Try each format for the matched pattern
                date_str = date_match.group(1)
                for date_format in formats:
                    try:
                        # Handle the special combined format case
                        if date_format.startswith('combined:'):
                            # Format like "2025.05.03-01.00.00" or "2025-05-03 01:00:00"
                            actual_format = date_format.split(':', 1)[1]
                            groups = date_match.groups()

                            if len(groups) >= 4:
                                # Combine date and time parts
                                date_part = groups[0]
                                hour = groups[1]
                                minute = groups[2]
                                second = groups[3]

                                if '-' in date_part:
                                    date_time_str = f"{date_part} {hour}:{minute}:{second}"
                                else:
                                    date_time_str = f"{date_part} {hour}:{minute}:{second}"

                                file_date = datetime.strptime(date_time_str, actual_format)
                                matched = True
                                break
                        else:
                            # Standard single-part format
                            file_date = datetime.strptime(date_str, date_format)
                            matched = True
                            break

                    except ValueError:
                        # Try next format
                        continue

                if matched:
                    break

            # If we didn't extract a date but file has "hourly" pattern, try to parse it specially
            if not matched and not include_hourly and re.search(r'hourly|(\d{2}\.\d{2}\.\d{2})', file_name, re.IGNORECASE):
                # Skip hourly files if requested
                continue

            # If we extracted a date, check if it's in range
            if file_date:
                if start_date <= file_date <= end_date:
                    filtered_files.append(file_path)
                    file_dates[file_path] = file_date
            else:
                # If no date found or parsing failed, include the file
                filtered_files.append(file_path)

        logger.info(f"Filtered to {len(filtered_files)} CSV files within date range {start_date} to {end_date}")

        ## Sort files by date, newest first, if dates were extracted
        if file_dates:
            return sorted(filtered_files, key=lambda f: file_dates.get(f, datetime.min), reverse=True)
        else:
            # Otherwise sort by name
            return sorted(filtered_files)

    @with_operation_tracking("find_csv_recursive")
    async def _find_csv_files_recursive(self, directory: str, max_depth: int = 10) -> List[str]:
        """Find CSV files recursively in a directory structure with enhanced map directory support

        This method is specialized for Emeralds Killfeed's directory structure where
        each subdirectory under the deathlogs path represents a different map.
        We need to search ALL map directories and collect ALL CSV files.

        The directory structure is usually:
        /hostname_serverid/actual1/deathlogs/world_0/*.csv
        /hostname_serverid/actual1/deathlogs/world_1/*.csv

        Args:
            directory: Directory to search (typically the deathlogs path)
            max_depth: Maximum recursion depth (increased to 10 to reach map subdirectories)

        Returns:
            List of CSV file paths from all map directories
        """
        logger.debug(f"Finding CSV files recursively (max_depth={max_depth})")
        all_csv_files = []

        try:
            # First, look for map subdirectories in this path
            await self.ensure_connected()

            try:
                # List all entries in the directory
                # These could be map directories or CSV files directly
                entries = await self._sftp_client.listdir(directory)
                logger.debug(f"Found {len(entries)} entries in directory - checking for map directories and CSV files")

                # Check each entry - could be a map directory or CSV directly
                for entry in entries:
                    if entry in ('.', '..'):
                        continue

                    full_path = os.path.join(directory, entry)
                    entry_info = await self.get_file_info(full_path)

                    if not entry_info:
                        logger.debug(f"Could not get info for {full_path}")
                        continue

                    # If it's a directory, it could be a map directory - search it for CSV files
                    if entry_info.get("is_dir", False):
                        logger.debug(f"Found potential map directory: {full_path}")
                        map_csv_files = await self.find_files_by_pattern(
                            full_path,
                            r'\d{4}[.-]\d{2}[.-]\d{2}.*\.csv$',  # Match date-formatted CSV files
                            recursive=True,
                            max_depth=8  # Increased depth for map subdirectories to handle deeper nesting
                        )

                        if map_csv_files:
                            logger.debug(f"Found {len(map_csv_files)} CSV files in map directory")
                            all_csv_files.extend(map_csv_files)
                        else:
                            # Try generic pattern as fallback within this map directory
                            generic_map_csvs = await self.find_files_by_pattern(
                                full_path,
                                r'\.csv$',  # Match any CSV file
                                recursive=True,
                                max_depth=8  # Increased depth for map subdirectories
                            )
                            if generic_map_csvs:
                                logger.debug(f"Found {len(generic_map_csvs)} generic CSV files in map directory")
                                all_csv_files.extend(generic_map_csvs)

                    # If it's a file and matches CSV pattern, add it directly
                    elif entry_info.get("is_file", False) and entry.lower().endswith('.csv'):
                        logger.debug(f"Found CSV file directly in directory")
                        all_csv_files.append(full_path)

            except Exception as list_err:
                logger.warning(f"Error listing directory {directory}: {list_err}")

            # If no map directories found, or as additional search, use standard pattern matching
            if not all_csv_files:
                logger.debug(f"No CSV files found in map directories, trying standard pattern search")

                # Use a specific pattern for Emeralds Killfeed CSV files
                # Format: YYYY.MM.DD-HH.MM.SS.csv or similar variations
                csv_files = await self.find_files_by_pattern(
                    directory, 
                    r'\d{4}[.-]\d{2}[.-]\d{2}.*\.csv$',  # Match date-formatted CSV files
                    recursive=True, 
                    max_depth=max_depth
                )

                if csv_files:
                    logger.debug(f"Found {len(csv_files)} date-formatted CSV files in standard search")
                    all_csv_files.extend(csv_files)
                else:
                    # Try a more generic pattern as fallback
                    generic_csvs = await self.find_files_by_pattern(
                        directory, 
                        r'\.csv$',  # Match any CSV file
                        recursive=True, 
                        max_depth=max_depth
                    )
                    if generic_csvs:
                        logger.debug(f"Found {len(generic_csvs)} generic CSV files in standard search")
                        all_csv_files.extend(generic_csvs)

            # Remove duplicates and sort files by name
            unique_files = list(set(all_csv_files))
            logger.info(f"Total CSV files found after deduplication: {len(unique_files)} (from {len(all_csv_files)} total)")

            # Log sample of found files
            if unique_files:
                sample = unique_files[:5] if len(unique_files) > 5 else unique_files
                logger.info(f"Sample CSV files: {sample}")

            return sorted(unique_files)

        except Exception as e:
            logger.error(f"Error in _find_csv_files_recursive for {directory}: {e}")
            logger.debug(f"Stack trace:\n{traceback.format_exc()}")
            return []

    @with_operation_tracking("read_csv")
    @retryable(max_retries=2, delay=1.0, backoff=1.5, 
               exceptions=(asyncio.TimeoutError, ConnectionError, OSError))
    async def read_csv_lines(
        self, 
        remote_path: str, 
        encoding: str = 'utf-8',
        timeout: Optional[float] = 30.0,
        fallback_encodings: List[str] = ['latin-1', 'cp1252', 'iso-8859-1']
    ) -> List[str]:
        """Read CSV file lines with robust error handling and encoding detection

        Features:
        - Automatic retries for network errors
        - Multiple encoding fallbacks
        - Timeout protection
        - Operation tracking
        - Detailed error reporting

        Args:
            remote_path: Remote file path
            encoding: Primary file encoding to try
            timeout: Operation timeout in seconds (None for no timeout)
            fallback_encodings: List of encodings to try if primary encoding fails

        Returns:
            List of lines from the CSV file
        """
        start_time = datetime.now()
        self.last_operation = f"read_csv_lines({remote_path})"
        self.last_error = None
        self.operation_count += 1

        logger.debug(f"Reading CSV file: {remote_path} with encoding: {encoding}")

        try:
            # Use timeout to prevent hanging
            if timeout:
                async with asyncio.timeout(timeout):
                    return await self._read_csv_lines_impl(remote_path, encoding, fallback_encodings)
            else:
                return await self._read_csv_lines_impl(remote_path, encoding, fallback_encodings)

        except asyncio.TimeoutError:
            elapsed = (datetime.now() - start_time).total_seconds()
            self.last_error = f"Operation timed out after {elapsed:.1f}s"
            logger.error(f"Reading CSV file {remote_path} timed out after {elapsed:.1f}s")
            raise

        except Exception as e:
            elapsed = (datetime.now() - start_time).total_seconds()
            self.last_error = f"{type(e).__name__}: {str(e)}"
            logger.error(f"Error reading CSV file {remote_path}: {e} (after {elapsed:.1f}s)")

            # Convert to retryable error types
            if isinstance(e, (OSError, IOError, ConnectionError, asyncio.TimeoutError)):
                raise  # Let retry decorator handle these types directly
            else:
                # Wrap in ConnectionError for retry handling
                raise ConnectionError(f"SFTP operation failed: {str(e)}")

    async def _read_csv_lines_impl(
        self, 
        remote_path: str, 
        encoding: str,
        fallback_encodings: List[str]
    ) -> List[str]:
        """Implementation of CSV file reading with encoding fallbacks"""
        # Download the file
        content = await self.download_file(remote_path)

        if not content:
            logger.warning(f"No content downloaded from {remote_path}")
            return []

        logger.debug(f"Downloaded {len(content)} bytes from {remote_path}")

        # Try the primary encoding first
        try:
            text = content.decode(encoding)
            lines = text.splitlines()
            logger.debug(f"Successfully decoded with {encoding}: {len(lines)} lines")
            return lines
        except UnicodeDecodeError:
            logger.debug(f"Failed to decode with {encoding}, trying fallbacks")

        # Try fallback encodings
        for fallback in fallback_encodings:
            try:
                text = content.decode(fallback)
                lines = text.splitlines()
                logger.debug(f"Successfully decoded with fallback {fallback}: {len(lines)} lines")
                return lines
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.debug(f"Error with fallback encoding {fallback}: {e}")
                continue

        # Try a desperate measure - replace invalid chars
        try:
            text = content.decode(encoding, errors='replace')
            lines = text.splitlines()
            logger.warning(f"Used {encoding} with error replacement on {remote_path}: {len(lines)} lines")
            return lines
        except Exception as e:
            logger.error(f"Final attempt to decode {remote_path} failed: {e}")
            return []

    async def get_file_size(self, remote_path: str) -> Optional[int]:
        """Get size of file in bytes

        Args:
            remote_path: Remote file path

        Returns:
            File size in bytes or None if error occurs
        """
        info = await self.get_file_info(remote_path)
        if info and "size" in info:
            return info["size"]
        return None

    @with_operation_tracking("get_latest_csv")
    @retryable(max_retries=2, delay=1.0, backoff=2.0, 
               exceptions=(asyncio.TimeoutError, ConnectionError, OSError))
    async def get_latest_csv_file(self) -> Optional[str]:
        """Find the most recent CSV file across all map subdirectories

        This method will search for CSV files in all map subdirectories within the deathlogs path:
        /hostname_serverid/actual1/deathlogs/*/

        Each subdirectory under deathlogs represents a different map, and we need to check
        all map directories to find the most recent CSV file across all maps.

        Returns:
            Path to the most recent CSV file or None if no files found
        """
        await self.ensure_connected()

        if not self._connected or not self._sftp_client:
            logger.error(f"Cannot get latest CSV file - not connected: {self.connection_id}")
            return None

        try:
            # Use original_server_id for path construction if available, otherwise fall back to server_id
            path_server_id = self.original_server_id if hasattr(self, 'original_server_id') and self.original_server_id else self.server_id
            # Log which server ID we're using for path construction only in debug mode
            logger.debug(f"Using server ID '{path_server_id}' for path construction")

            # Build base path with hostname_serverid structure
            server_dir = f"{self.hostname.split(':')[0]}_{path_server_id}"
            deathlogs_path = os.path.join("/", server_dir, "actual1", "deathlogs")

            # Check if the log file exists directly
            if await self.exists(log_file_path):
                logger.info(f"Found log file at: {log_file_path}")
                return log_file_path

            # Define common map subdirectory names to check directly (prioritize these)
            known_map_names = ["world_0", "world0", "map_0", "map0", "main", "default"]
            logger.debug(f"Prioritizing known map directories: {known_map_names}")

            logger.debug(f"Searching for map directories in: {deathlogs_path}")

            # List of all CSV files across all map directories
            all_csv_files = []

            # First check if deathlogs_path exists
            if not await self.exists(deathlogs_path):
                logger.warning(f"Main deathlogs path does not exist: {deathlogs_path}")

                # Try alternate paths
                alternate_paths = [
                    os.path.join("/", server_dir, "deathlogs"),
                    os.path.join("/", server_dir, "logs"),
                    os.path.join("/", server_dir, "Logs"),
                    os.path.join("/", "deathlogs"),
                    os.path.join("/", server_dir)
                ]

                for alt_path in alternate_paths:
                    logger.debug(f"Trying alternate path: {alt_path}")
                    if await self.exists(alt_path):
                        deathlogs_path = alt_path
                        logger.debug(f"Using alternate path: {deathlogs_path}")
                        break
                else:
                    logger.error(f"Could not find any valid path for server {self.server_id}")
                    return None

            # First try to get entries in the deathlogs directory - these could be map subdirectories
            try:
                entries = await self._sftp_client.listdir(deathlogs_path)
                logger.debug(f"Found {len(entries)} entries in deathlogs path")

                # Check for CSV files directly in deathlogs path
                direct_csv_pattern = re.compile(r'\.csv$', re.IGNORECASE)
                direct_csvs = [
                    os.path.join(deathlogs_path, entry)
                    for entry in entries
                    if direct_csv_pattern.search(entry)
                ]

                if direct_csvs:
                    logger.debug(f"Found {len(direct_csvs)} CSV files directly in deathlogs path")
                    all_csv_files.extend(direct_csvs)

                # Check each entry to find map directories
                map_directories = []

                for entry in entries:
                    if entry in ('.', '..'):
                        continue

                    entry_path = os.path.join(deathlogs_path, entry)

                    try:
                        entry_info = await self.get_file_info(entry_path)
                        if not entry_info:
                            continue

                        # If this is a directory, it might be a map directory
                        if entry_info.get("is_dir", False):
                            logger.debug(f"Found potential map directory: {entry_path}")
                            map_directories.append(entry_path)
                    except Exception as entry_err:
                        logger.debug(f"Error checking entry {entry_path}: {entry_err}")

                logger.debug(f"Found {len(map_directories)} potential map directories")

                # First, prioritize checking known map names directly
                for known_map in known_map_names:
                    known_map_path = os.path.join(deathlogs_path, known_map)
                    logger.debug(f"Checking known map directory: {known_map_path}")

                    if await self.exists(known_map_path):
                        logger.debug(f"Known map directory exists")
                        # Add to map directories if not already there
                        if known_map_path not in map_directories:
                            map_directories.insert(0, known_map_path)  # Add at beginning to prioritize

                # Search for CSV files in each map directory
                for map_dir in map_directories:
                    try:
                        logger.debug(f"Searching for CSV files in map directory")
                        map_csv_files = await self.find_files_by_pattern(
                            map_dir,
                            r'\.csv$',
                            recursive=True,
                            max_depth=3  # Maps shouldn't need deep recursion
                        )

                        if map_csv_files:
                            logger.debug(f"Found {len(map_csv_files)} CSV files in map directory")
                            all_csv_files.extend(map_csv_files)
                    except Exception as map_err:
                        logger.warning(f"Error searching map directory: {map_err}")

            except Exception as list_err:
                logger.warning(f"Error listing deathlogs directory: {list_err}")

            # If no files found through map directories, try general recursive search
            if not all_csv_files:
                logger.debug(f"No CSV files found in map directories, trying general search")
                try:
                    csv_files = await self.find_csv_files(
                        directory=deathlogs_path,
                        recursive=True,
                        max_depth=8,  # Search deeper
                        include_hourly=True
                    )

                    if csv_files:
                        logger.debug(f"Found {len(csv_files)} CSV files with general search")
                        all_csv_files.extend(csv_files)
                except Exception as search_err:
                    logger.warning(f"Error in general search: {search_err}")

            # If still no files found, try checking parent directory
            if not all_csv_files:
                parent_path = os.path.dirname(deathlogs_path)
                logger.debug(f"Trying parent directory search")

                try:
                    parent_csvs = await self.find_csv_files(
                        directory=parent_path,
                        recursive=True,
                        max_depth=4,
                        include_hourly=True
                    )

                    if parent_csvs:
                        logger.debug(f"Found {len(parent_csvs)} CSV files in parent directory")
                        all_csv_files.extend(parent_csvs)
                except Exception as parent_err:
                    logger.warning(f"Error searching parent directory: {parent_err}")

            # If we still have no CSV files, give up
            if not all_csv_files:
                logger.error(f"No CSV files found for server {self.server_id} after exhaustive search")
                return None

            # Remove duplicates
            all_csv_files = list(set(all_csv_files))
            logger.debug(f"Found {len(all_csv_files)} unique CSV files across all map directories")

            # First try to find the newest file by date in the filename
            date_pattern = re.compile(r'(\d{4})[.-](\d{2})[.-](\d{2})(?:[.-](\d{2}))?(?:[.-](\d{2}))?(?:[.-](\d{2}))?\.csv$')
            newest_file = None
            newest_date = None

            for csv_file in all_csv_files:
                try:
                    match = date_pattern.search(os.path.basename(csv_file))
                    if match:
                        # Extract date components, handling optional time components
                        year = int(match.group(1))
                        month = int(match.group(2))
                        day = int(match.group(3))
                        hour = int(match.group(4)) if match.group(4) else 0
                        minute = int(match.group(5)) if match.group(5) else 0
                        second = int(match.group(6)) if match.group(6) else 0

                        file_date = datetime(year, month, day, hour, minute, second)

                        if newest_date is None or file_date > newest_date:
                            newest_date = file_date
                            newest_file = csv_file
                            logger.debug(f"New newest file by date: {csv_file} ({file_date})")
                except Exception as date_err:
                    logger.debug(f"Error parsing date from {csv_file}: {date_err}")

            # If we found a file with a valid date, return it
            if newest_file:
                logger.debug(f"Found newest CSV file by date in filename: {newest_file}")
                return newest_file

            # Fall back to modification time if date parsing failed
            newest_file = None
            newest_time = 0

            for file_path in all_csv_files:
                try:
                    file_info = await self.get_file_info(file_path)
                    if file_info and 'mtime' in file_info:
                        mtime = file_info['mtime']
                        if mtime > newest_time:
                            newest_time = mtime
                            newest_file = file_path
                except Exception as stat_e:
                    logger.debug(f"Error getting stats: {stat_e}")

            if newest_file:
                logger.debug(f"Found most recent CSV file by mtime")
                return newest_file

            # Last resort: sort alphabetically and return the last one
            logger.debug(f"Using alphabetical sort as fallback method")
            sorted_files = sorted(all_csv_files)
            return sorted_files[-1] 

        except Exception as e:
            logger.error(f"Error in get_latest_csv_file: {e}")
            logger.debug(f"Stack trace:\n{traceback.format_exc()}")
            return None

# Add SFTPManager as alias for SFTPClient to maintain backward compatibility
# This allows code using either class name to work properly
# This class is intentionally removed as it's been replaced by the more robust 
# SFTPManager class defined below. All calls to this class will now use the full-featured 
# implementation that properly handles hostname:port format strings and has comprehensive 
# error handling