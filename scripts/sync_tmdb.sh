#!/bin/bash

# TMDB Data Pipeline Synchronization Script
# This script can be run via cron to keep the database synchronized

# Load environment variables from .env file
if [ -f ".env" ]; then
    # Use a more robust method to load environment variables
    set -a  # automatically export all variables
    source .env 2>/dev/null || true
    set +a  # stop automatically exporting
fi

# Set the project directory from environment variable or use default
PROJECT_DIR="${PROJECT_DIR:-/Users/jeevanparmar/Uni/Capstone/Back_End}"
cd "$PROJECT_DIR"

# Activate virtual environment
source venv/bin/activate

# Log file for this run
LOG_FILE="logs/sync_$(date +%Y%m%d_%H%M%S).log"

echo "Starting TMDB synchronization at $(date)" | tee -a "$LOG_FILE"

# Update all existing movies
echo "Updating existing movies..." | tee -a "$LOG_FILE"
python -m src.etl.update_tmdb_data --update --batch-size 100 2>&1 | tee -a "$LOG_FILE"

# Add new movies from the last week
echo "Adding new movies from the last week..." | tee -a "$LOG_FILE"
python -m src.etl.update_tmdb_data --add-new-movies --time-period week 2>&1 | tee -a "$LOG_FILE"

echo "TMDB synchronization completed at $(date)" | tee -a "$LOG_FILE"
echo "----------------------------------------" | tee -a "$LOG_FILE" 