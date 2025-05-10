#!/bin/bash
# Discord Bot Launcher
# This script starts the Discord bot with proper environment setup

# Set up Python environment
export PYTHONPATH="."
export PYTHONUNBUFFERED="1"

# Log start time
echo "Starting Discord bot at $(date)" >> bot_startup.log

# Print banner
echo "====================================================="
echo "  Tower of Temptation PvP Statistics Discord Bot"
echo "  Starting from Replit Run Button"
echo "====================================================="

# Execute the bot with proper error handling
python bot.py

# Check exit code
if [ $? -ne 0 ]; then
    echo "Bot exited with error code $?" >> bot_error.log
    echo "Error occurred at $(date)" >> bot_error.log
    echo "See logs for details" >> bot_error.log
fi

echo "Bot process ended at $(date)" >> bot_startup.log