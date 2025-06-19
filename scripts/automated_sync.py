#!/usr/bin/env python3
"""
Automated TMDB Data Pipeline Synchronization
This script runs scheduled synchronization tasks to keep the database updated.
"""

import schedule
import time
import logging
import subprocess
import sys
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/automated_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_sync_command(command, description):
    """Run a synchronization command and log the results."""
    try:
        logger.info(f"Starting: {description}")
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent
        )
        
        if result.returncode == 0:
            logger.info(f"Successfully completed: {description}")
            if result.stdout:
                logger.info(f"Output: {result.stdout}")
        else:
            logger.error(f"Failed: {description}")
            logger.error(f"Error: {result.stderr}")
            
    except Exception as e:
        logger.error(f"Exception running {description}: {str(e)}")

def daily_update():
    """Daily update task - update existing movies and add new ones from the last day."""
    logger.info("=" * 50)
    logger.info("Starting daily TMDB synchronization")
    
    # Update existing movies
    run_sync_command(
        "python -m src.etl.update_tmdb_data --update --batch-size 100",
        "Update existing movies"
    )
    
    # Add new movies from the last day
    run_sync_command(
        "python -m src.etl.update_tmdb_data --add-new-movies --time-period day",
        "Add new movies from the last day"
    )
    
    logger.info("Daily TMDB synchronization completed")
    logger.info("=" * 50)

def weekly_update():
    """Weekly update task - more comprehensive update."""
    logger.info("=" * 50)
    logger.info("Starting weekly TMDB synchronization")
    
    # Update existing movies
    run_sync_command(
        "python -m src.etl.update_tmdb_data --update --batch-size 100",
        "Update existing movies"
    )
    
    # Add new movies from the last week
    run_sync_command(
        "python -m src.etl.update_tmdb_data --add-new-movies --time-period week",
        "Add new movies from the last week"
    )
    
    logger.info("Weekly TMDB synchronization completed")
    logger.info("=" * 50)

def monthly_update():
    """Monthly update task - comprehensive update including older movies."""
    logger.info("=" * 50)
    logger.info("Starting monthly TMDB synchronization")
    
    # Update existing movies
    run_sync_command(
        "python -m src.etl.update_tmdb_data --update --batch-size 100",
        "Update existing movies"
    )
    
    # Add new movies from the last month
    run_sync_command(
        "python -m src.etl.update_tmdb_data --add-new-movies --time-period month",
        "Add new movies from the last month"
    )
    
    logger.info("Monthly TMDB synchronization completed")
    logger.info("=" * 50)

def setup_schedule():
    """Set up the scheduling for different sync tasks."""
    # Daily updates at 2 AM
    schedule.every().day.at("02:00").do(daily_update)
    
    # Weekly updates on Sunday at 3 AM
    schedule.every().sunday.at("03:00").do(weekly_update)
    
    # Monthly updates on the 1st of each month at 4 AM
    # Note: schedule library doesn't support monthly scheduling directly
    # We'll use a workaround by checking the date in the daily job
    schedule.every().day.at("04:00").do(check_monthly_update)
    
    logger.info("Scheduled tasks:")
    logger.info("- Daily updates: 2:00 AM")
    logger.info("- Weekly updates: Sunday 3:00 AM")
    logger.info("- Monthly updates: 1st of month 4:00 AM")

def check_monthly_update():
    """Check if it's the first of the month and run monthly update if so."""
    today = datetime.now()
    
    if today.day == 1:
        logger.info("First day of month detected - running monthly update")
        monthly_update()
    else:
        logger.info(f"Not first day of month (day {today.day}) - skipping monthly update")

def main():
    """Main function to run the automated scheduler."""
    logger.info("Starting TMDB Automated Synchronization Service")
    
    # Set up the schedule
    setup_schedule()
    
    # Run initial sync
    logger.info("Running initial synchronization...")
    daily_update()
    
    # Keep the scheduler running
    logger.info("Scheduler is running. Press Ctrl+C to stop.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Stopping automated synchronization service")
        sys.exit(0)

if __name__ == "__main__":
    main() 