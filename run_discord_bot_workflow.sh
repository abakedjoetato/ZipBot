#!/bin/bash
# Discord Bot Workflow Runner
# This script is designed to run the Discord bot in a Replit workflow

# Set up Python environment
export PYTHONPATH="."
export PYTHONUNBUFFERED="1"

# Log the start
echo "Starting Discord bot workflow at $(date)" > workflow_log.txt

# Print banner to workflow console
echo "====================================================="
echo "  Tower of Temptation PvP Statistics Discord Bot"
echo "  Running in Replit Workflow"
echo "====================================================="

# Create marker file to indicate we're running in a workflow
touch .running_in_workflow

# Execute the bot with proper Python path
python main.py

# Check exit code
if [ $? -ne 0 ]; then
    echo "Bot exited with error code $?" >> workflow_error.log
    echo "Error occurred at $(date)" >> workflow_error.log
    echo "See logs for details" >> workflow_error.log
fi

echo "Bot workflow process ended at $(date)" >> workflow_log.txt

# Remove the workflow marker file
rm -f .running_in_workflow