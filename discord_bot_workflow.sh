#!/bin/bash

# Discord Bot Workflow Script
# This script runs the Discord bot test script

echo "Starting Discord Bot Test Workflow..."
echo "Timestamp: $(date)"
echo "-----------------------------------"

# Make the test script executable
chmod +x test_discord_bot.py

# Run the test script
python test_discord_bot.py

# Workflow completion
echo "-----------------------------------"
echo "Discord Bot Test Workflow completed."
echo "Timestamp: $(date)"
echo "Check discord_test.log for results."