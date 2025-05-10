#!/bin/bash

# Run Discord Bot Workflow
# This script runs the Discord bot and tests CSV processing

echo "Starting Discord Bot Test..."
echo "Time: $(date)"
echo "---------------------------------"

# Verify bot token is set
if [ -z "$DISCORD_BOT_TOKEN" ]; then
  echo "ERROR: DISCORD_BOT_TOKEN environment variable is not set."
  echo "Please ensure the bot token is properly configured."
  exit 1
fi

# Run the Discord bot with CSV testing
echo "Starting Discord bot with CSV testing..."
python run_bot.py

echo "---------------------------------"
echo "Discord Bot Test completed."
echo "Time: $(date)"