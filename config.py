"""
Configuration management for the Discord bot
"""
import os
import json
import logging

logger = logging.getLogger(__name__)

# Default configuration values
DEFAULT_CONFIG = {
    "prefix": "!",
    "description": "A Discord bot powered by discord.py",
    "activity": "with Discord",
    "activity_type": "playing",
    "log_level": "INFO"
}

def load_config():
    """
    Load configuration from JSON file or environment variables
    
    Priority:
    1. Environment variables
    2. config.json file
    3. Default values
    
    Returns:
        dict: Bot configuration
    """
    config = DEFAULT_CONFIG.copy()
    
    # Try to load from config.json
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                file_config = json.load(f)
                config.update(file_config)
            logger.info("Loaded configuration from config.json")
        except Exception as e:
            logger.error(f"Error loading config.json: {e}")
    else:
        logger.warning("No config.json found, using default values or environment variables")
    
    # Override with environment variables if they exist
    env_prefix = os.getenv("BOT_PREFIX")
    if env_prefix:
        config["prefix"] = env_prefix
        
    env_description = os.getenv("BOT_DESCRIPTION")
    if env_description:
        config["description"] = env_description
        
    env_activity = os.getenv("BOT_ACTIVITY")
    if env_activity:
        config["activity"] = env_activity
        
    env_activity_type = os.getenv("BOT_ACTIVITY_TYPE")
    if env_activity_type:
        config["activity_type"] = env_activity_type
        
    env_log_level = os.getenv("BOT_LOG_LEVEL")
    if env_log_level:
        config["log_level"] = env_log_level
    
    return config
