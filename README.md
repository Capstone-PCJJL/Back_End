# TMDB Data Pipeline

This project implements a data pipeline for fetching, processing, and storing movie data from The Movie Database (TMDB) API. The pipeline includes ETL processes, data loading, and update mechanisms.

## Features

- Initial data loading from TMDB API
- Incremental updates for existing movies
- Search and add new movies
- Add new movies based on release date
- Efficient batch processing
- Rate limiting and error handling
- Database storage with MySQL

## Prerequisites

- Python 3.8+
- MySQL 8.0+
- TMDB API key and bearer token

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd <repository-directory>
```

2. Create and activate a virtual environment:

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
.\venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Updating requirements.txt

```bash
pip freeze > requirements.txt
```

5. Set up environment variables:
   Create a `.env` file in the project root with:

```
API_KEY=your_tmdb_api_key
TMDB_BEARER_TOKEN=your_tmdb_bearer_token
DB_HOST=localhost
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_NAME=your_db_name
```

## Database Synchronization Guide

### Initial Setup

1. **First-time Database Load**:
```bash
python -m src.etl.tmdb_etl --initial
```
Optional parameters:
- `--batch-size`: Number of movies to process in each batch (default: 100)
- `--max-workers`: Maximum number of parallel workers (default: 10)
- `--test-year`: Test with a single year (e.g., 2024)

### Regular Updates

1. **Update All Movies**:
```bash
# Update all movies in the database
python -m src.etl.update_tmdb_data --update

# Update with custom batch size
python -m src.etl.update_tmdb_data --update --batch-size 50
```

2. **Update Specific Movie**:
```bash
python -m src.etl.update_tmdb_data --update 123456
```

3. **Add New Movies**:
```bash
# Add movies from the last day
python -m src.etl.update_tmdb_data --add-new-movies --time-period day

# Add movies from the last week
python -m src.etl.update_tmdb_data --add-new-movies --time-period week

# Add movies from the last month
python -m src.etl.update_tmdb_data --add-new-movies --time-period month

# Add movies from the last X days (e.g., 200 days)
python -m src.etl.update_tmdb_data --add-new-movies --time-period 200

# Add movies since the last update in database
python -m src.etl.update_tmdb_data --add-new-movies
```

4. **Search and Add Specific Movies**:
```bash
# Search by movie title
python -m src.etl.update_tmdb_data --search "Inception"

# Search by movie ID
python -m src.etl.update_tmdb_data --search 27205
```

### Recommended Update Schedule

1. **Daily Updates**:
```bash
# Update all movies and add new ones from the last day
python -m src.etl.update_tmdb_data --update
python -m src.etl.update_tmdb_data --add-new-movies --time-period day
```

2. **Weekly Updates**:
```bash
# Update all movies and add new ones from the last week
python -m src.etl.update_tmdb_data --update
python -m src.etl.update_tmdb_data --add-new-movies --time-period week
```

3. **Monthly Deep Update**:
```bash
# Update all movies and add new ones from the last month
python -m src.etl.update_tmdb_data --update
python -m src.etl.update_tmdb_data --add-new-movies --time-period month
```

## Automation Scripts

### Shell Script Automation

The shell script (`scripts/sync_tmdb.sh`) provides a simple way to run synchronization tasks manually or via cron.

#### Manual Execution:
```bash
# Make the script executable (first time only)
chmod +x scripts/sync_tmdb.sh

# Run the synchronization manually
./scripts/sync_tmdb.sh
```

#### Cron Job Setup:
```bash
# Edit crontab
crontab -e

# Add one of these lines for different schedules:

# Daily at 2 AM
0 2 * * * /path/to/your/project/scripts/sync_tmdb.sh

# Weekly on Sunday at 3 AM
0 3 * * 0 /path/to/your/project/scripts/sync_tmdb.sh

# Every 6 hours
0 */6 * * * /path/to/your/project/scripts/sync_tmdb.sh
```

### Python Scheduler Automation

The Python script (`scripts/automated_sync.py`) provides a more sophisticated scheduling system with different update frequencies.

#### Manual Execution:
```bash
# Run the automated scheduler
python scripts/automated_sync.py
```

#### Background Service:
```bash
# Run in background
nohup python scripts/automated_sync.py > logs/automated_sync_service.log 2>&1 &

# Check if it's running
ps aux | grep automated_sync.py

# Stop the service
pkill -f automated_sync.py
```

#### Systemd Service (Linux):
Create `/etc/systemd/system/tmdb-sync.service`:
```ini
[Unit]
Description=TMDB Data Pipeline Synchronization
After=network.target

[Service]
Type=simple
User=your_username
WorkingDirectory=/path/to/your/project
ExecStart=/path/to/your/project/venv/bin/python scripts/automated_sync.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start the service:
```bash
sudo systemctl enable tmdb-sync
sudo systemctl start tmdb-sync
sudo systemctl status tmdb-sync
```

## Initial CSV Upload

During the initial setup, you can upload existing CSV files to populate the database before running the TMDB API synchronization.

### CSV File Requirements

The pipeline expects CSV files with the following structure:

1. **Movies CSV** (`movies.csv`):
   - Required columns: `id`, `title`, `overview`, `release_date`, `popularity`, `vote_average`, `vote_count`
   - Optional columns: `budget`, `revenue`, `poster_path`, `backdrop_path`

2. **Credits CSV** (`credits.csv`):
   - Required columns: `movie_id`, `person_id`, `name`, `character`, `order`, `department`, `job`
   - For actors: `character` and `order` should be populated
   - For crew: `department` and `job` should be populated

3. **People CSV** (`people.csv`):
   - Required columns: `id`, `name`, `profile_path`
   - Optional columns: `gender`, `known_for_department`

4. **Genres CSV** (`genres.csv`):
   - Required columns: `movie_id`, `genre_id`, `genre_name`

### Upload Process

1. **Prepare CSV Files**:
   ```bash
   # Create a data directory for your CSV files
   mkdir -p data/csv
   
   # Place your CSV files in the data/csv directory
   # - data/csv/movies.csv
   # - data/csv/credits.csv
   # - data/csv/people.csv
   # - data/csv/genres.csv
   ```

2. **Load CSV Data**:
   ```bash
   # Load all CSV files into the database
   python -m src.etl.load_tmdb_csvs
   
   # Load specific CSV file
   python -m src.etl.load_tmdb_csvs --file movies.csv
   
   # Load with custom batch size
   python -m src.etl.load_tmdb_csvs --batch-size 500
   ```

3. **Verify Data Loading**:
   ```bash
   # Check database connection and schema
   python test_db_connection.py
   
   # Check schema structure
   python -m src.scripts.check_schema
   ```

4. **Run TMDB Synchronization**:
   ```bash
   # After loading CSVs, run the initial TMDB sync
   python -m src.etl.tmdb_etl --initial
   ```

### CSV Validation

The loader includes validation for:
- Required columns presence
- Data type validation
- Date format validation
- Empty field handling
- Duplicate record detection

Check the logs in `logs/tmdb_load.log` for any validation errors or warnings.

## Testing Automation Scripts

Before setting up automated scheduling, it's important to test that the scripts work correctly in your environment.

### Testing Shell Script

1. **Manual Test Run**:
```bash
# Make sure the script is executable
chmod +x scripts/sync_tmdb.sh

# Run the script manually and watch the output
./scripts/sync_tmdb.sh

# Check the generated log file
tail -f logs/sync_$(date +%Y%m%d)*.log
```

2. **Test with Verbose Output**:
```bash
# Add debug output to see what's happening
bash -x scripts/sync_tmdb.sh
```

3. **Test Environment Variables**:
```bash
# Check if environment variables are loaded correctly
source scripts/sync_tmdb.sh
echo "API_KEY: $API_KEY"
echo "DB_HOST: $DB_HOST"
```

### Testing Python Scheduler

1. **Quick Test Run**:
```bash
# Run the scheduler for a few minutes to test (macOS compatible)
# Option 1: Use gtimeout if you have coreutils installed
gtimeout 300 python scripts/automated_sync.py

# Option 2: Use a simple background process with sleep
python scripts/automated_sync.py &
PID=$!
sleep 300
kill $PID

# Option 3: Run manually and stop with Ctrl+C after a few minutes
python scripts/automated_sync.py
```

2. **Test Individual Functions**:
```bash
# Create a test script to run individual functions
python -c "
from scripts.automated_sync import daily_update, weekly_update
import logging

# Set up logging to see output
logging.basicConfig(level=logging.INFO)

print('Testing daily update...')
daily_update()

print('Testing weekly update...')
weekly_update()
"
```

3. **Test with Custom Schedule**:
```bash
# Modify the script temporarily to run every minute for testing
# Edit scripts/automated_sync.py and change the schedule to:
# schedule.every(1).minutes.do(daily_update)
# Then run for a few minutes
python scripts/automated_sync.py
```

### Monitoring and Verification

1. **Check Log Files**:
```bash
# Monitor all relevant log files
tail -f logs/*.log

# Check specific log files
tail -f logs/automated_sync.log
tail -f logs/tmdb_api.log
tail -f logs/tmdb_etl.log
```

2. **Database Verification**:
```bash
# Check if new data was added/updated
python -c "
from src.database.db_manager import DatabaseManager
db = DatabaseManager()

# Check recent movies
movies = db.execute_query('SELECT COUNT(*) as count FROM movies')
print(f'Total movies: {movies[0][0]}')

# Check recent updates
recent = db.execute_query('SELECT COUNT(*) as count FROM movies WHERE updated_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)')
print(f'Movies updated in last hour: {recent[0][0]}')
"
```

3. **API Rate Limit Check**:
```bash
# Check if API calls are working
python -c "
from src.api.tmdb_client import TMDBClient
client = TMDBClient()

# Test API connection
try:
    movie = client.get_movie(550)  # Fight Club
    print(f'API test successful: {movie[\"title\"]}')
except Exception as e:
    print(f'API test failed: {e}')
"
```

### Common Issues and Solutions

1. **Environment Variables Not Loaded**:
```bash
# Check if .env file exists and has correct format
cat .env

# Test environment variable loading
source .env && echo "API_KEY: $API_KEY"
```

2. **Virtual Environment Issues**:
```bash
# Verify virtual environment is activated
which python
echo $VIRTUAL_ENV

# Reinstall dependencies if needed
pip install -r requirements.txt
```

3. **Database Connection Issues**:
```bash
# Test database connection
python test_db_connection.py

# Check database credentials
python -c "
from sqlconnection import get_db_connection
try:
    conn = get_db_connection()
    print('Database connection successful')
    conn.close()
except Exception as e:
    print(f'Database connection failed: {e}')
"
```

4. **Permission Issues**:
```bash
# Check script permissions
ls -la scripts/

# Fix permissions if needed
chmod +x scripts/sync_tmdb.sh
chmod +x scripts/automated_sync.py
```

### macOS-Specific Testing

If you're on macOS, some commands may need alternatives:

1. **Install coreutils for timeout command**:
```bash
# Install coreutils via Homebrew
brew install coreutils

# Now you can use gtimeout instead of timeout
gtimeout 300 python scripts/automated_sync.py
```

2. **Alternative to timeout without installing additional tools**:
```bash
# Run in background and kill after specified time
python scripts/automated_sync.py &
PID=$!
sleep 300  # Wait 5 minutes
kill $PID 2>/dev/null || true
```

3. **Check if coreutils is installed**:
```bash
# Check if gtimeout is available
which gtimeout

# If not available, use the background process method above
```

### Dry Run Testing

For safe testing without making actual API calls or database changes:

1. **Create a Test Environment**:
```bash
# Create a test database
mysql -u root -p -e "CREATE DATABASE tmdb_test;"

# Update .env with test database
echo "DB_NAME=tmdb_test" >> .env
```

2. **Test with Limited Data**:
```bash
# Test with a small batch size and limited pages
python -m src.etl.update_tmdb_data --update --batch-size 5
python -m src.etl.update_tmdb_data --add-new-movies --time-period day
```

### Success Indicators

Your automation is working correctly if you see:

1. **Log Messages**:
   - "Starting TMDB synchronization"
   - "Successfully completed: Update existing movies"
   - "Successfully completed: Add new movies"
   - No error messages

2. **Database Changes**:
   - New movies added to database
   - Existing movies updated with new data
   - Updated timestamps in the database

3. **File System**:
   - New log files created with timestamps
   - No permission errors
   - Scripts execute without hanging

4. **API Usage**:
   - Successful API calls (check TMDB dashboard)
   - No rate limit errors
   - Reasonable response times

## Data Structure

The pipeline processes and stores the following data:

### Movies

- Basic movie information (title, overview, release date, etc.)
- Ratings and popularity metrics
- Budget and revenue information
- Poster and backdrop paths

### Credits

- Top 8 actors for each movie
- Main director for each movie
- Character names and credit order for actors
- Department and job information for crew

### People

- Basic person information (name, profile path)
- Gender and known department
- Only includes actors and directors

### Genres

- Movie-genre associations
- Genre names

## Database Schema

The database includes the following tables:

- `movies`: Stores movie information
- `credits`: Stores movie credits (actors and directors)
- `people`: Stores person information
- `genres`: Stores movie-genre associations

## Error Handling and Logging

- Comprehensive error handling for API requests
- Rate limiting to avoid API restrictions
- Detailed logging of all operations
- Batch processing with retry mechanisms
- Memory management for large datasets

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
