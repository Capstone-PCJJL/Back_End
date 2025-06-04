# Movie Data ETL System

A comprehensive ETL (Extract, Transform, Load) system for managing movie data from The Movie Database (TMDB).

## Features

- Initial data loading from TMDB
- Processing of recent changes
- Handling of missing movies
- Interactive movie search with fuzzy matching
- YAML-based review process
- Comprehensive error handling
- Detailed logging

## Dependencies

- Python 3.8+
- SQLAlchemy
- TMDB API key
- Other requirements listed in `requirements.txt`

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file with your TMDB API key:
```
API_KEY=your_tmdb_api_key_here
```

## Commands

### Initial Data Load
Load movies from the latest year to the current year:
```bash
python -m src.etl.tmdb_etl init
```

Load movies for a specific year range:
```bash
python -m src.etl.tmdb_etl init --start-year 2020 --end-year 2023
```

### Process Changes
Process changes from the last 24 hours:
```bash
python -m src.etl.tmdb_etl changes
```

Process changes from a specific number of days:
```bash
python -m src.etl.tmdb_etl changes --days 7
```

### Process Missing Movies
Fetch missing movies since the last run:
```bash
python -m src.etl.tmdb_etl missing
```

Fetch missing movies after a specific date:
```bash
python -m src.etl.tmdb_etl missing --after-date 2023-01-01
```

### Search Movies
Search by movie name (with fuzzy matching):
```bash
python -m src.etl.tmdb_etl search "star wars"
```

Search by exact TMDB ID:
```bash
python -m src.etl.tmdb_etl search 11 --by id
```

The search command provides:
- Fuzzy matching for name searches
- Match scores for each result
- Movie overviews and release dates
- Interactive selection of the desired movie

### YAML Processing

#### Fetch to YAML
Fetch movies to a YAML file:
```bash
python -m src.etl.tmdb_etl fetch --type initial
python -m src.etl.tmdb_etl fetch --type missing
python -m src.etl.tmdb_etl fetch --type changes
```

#### Review YAML
Review movies in a YAML file:
```bash
python -m src.etl.tmdb_etl review --file data/yaml/tmdb_initial_20240101_120000.yaml
```

#### Load from YAML
Load approved movies from YAML:
```bash
python -m src.etl.tmdb_etl load --file data/yaml/tmdb_initial_20240101_120000.yaml
```

#### Combined Command
Fetch, review, and load in one command:
```bash
python -m src.etl.tmdb_etl fetch-review-load --type initial
```

## YAML File Structure

```yaml
metadata:
  batch_type: initial/missing/changes
  batch_id: optional_identifier
  timestamp: YYYYMMDD_HHMMSS
  movie_count: number_of_movies
movies:
  - title: Movie Title
    tmdb_id: 12345
    release_date: YYYY-MM-DD
    overview: Movie description
    approval_status: yes/no/skip
    review_date: YYYY-MM-DD HH:MM:SS
```

## Review Process

1. **Fetch**: Movies are fetched and saved to a YAML file
2. **Review**: Each movie is reviewed and marked as:
   - `yes`: Approved for loading
   - `no`: Rejected
   - `skip`: Skip for now
3. **Load**: Only approved movies are loaded into the database

## File Naming Convention

YAML files follow the pattern:
```
tmdb_{batch_type}_{batch_id}_{timestamp}.yaml
```

Example:
```
tmdb_initial_2023_20240101_120000.yaml
```

## Archiving

Reviewed YAML files are automatically moved to:
```
data/yaml/archived/
```

## Logging

The system provides detailed logging for:
- API requests
- Database operations
- Error handling
- Progress tracking

## Recent Improvements

- Added fuzzy matching for movie searches
- Enhanced error handling
- Improved progress tracking
- Added support for batch processing
- Implemented YAML-based review process

## Error Handling

The system includes comprehensive error handling for:
- API failures
- Database errors
- Invalid data
- Network issues
- Rate limiting

## Database Schema

The system uses the following tables:
- `movies_raw`: Raw movie data
- `movies`: Processed movie data
- `tmdb_movies`: TMDB-specific movie data
- `genres`: Movie genres
- `movie_genres`: Movie-genre relationships
- `tmdb_movie_genres`: TMDB movie-genre relationships

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Data Sources

### MovieLens Dataset
- Used for initial movie data
- Provides base movie information
- Used to determine the latest year for initial loads

### TMDB API
- Provides movie metadata, credits, videos, reviews, keywords
- Contains release dates, content ratings, and watch providers
- Used for updates and missing data

## YAML Processing

The system uses YAML files as an intermediate step for human review before loading data into the database. This is particularly useful for changes and missing movies that require approval.

### YAML Commands

```bash
# Fetch movies to YAML for review
python -m src.etl.tmdb_etl fetch --type missing
python -m src.etl.tmdb_etl fetch --type changes
python -m src.etl.tmdb_etl fetch --type search --query "movie name"

# Review movies in YAML
python -m src.etl.tmdb_etl review --file data/yaml/tmdb_missing_20240315_123456.yaml

# Load approved movies from YAML
python -m src.etl.tmdb_etl load --file data/yaml/tmdb_missing_20240315_123456.yaml

# Combined fetch, review, and load
python -m src.etl.tmdb_etl fetch-review-load --type missing
```

### YAML File Structure
```yaml
metadata:
  batch_type: missing  # or changes, search
  batch_id: 2024-03-15
  timestamp: 20240315_123456
  movie_count: 100

movies:
  - id: 12345
    title: "Movie Title"
    release_date: "2024-03-15"
    overview: "Movie description..."
    adult: false
    genres:
      - name: "Action"
      - name: "Drama"
    approval_status: null  # Will be set during review
    review_date: null     # Will be set during review
```

### Review Process
1. **Fetch to YAML**:
   - Movies are fetched from TMDB
   - Filtered for adult content
   - Saved to YAML with metadata
   - Files are stored in `data/yaml/` directory

2. **Review YAML**:
   - Interactive review of each movie
   - Options: approve (yes), reject (no), or skip
   - Updates approval status and review date
   - Saves changes back to YAML

3. **Load from YAML**:
   - Only approved movies are loaded
   - Creates database records
   - Processes genres and relationships
   - Archives YAML file after successful load

### YAML File Naming
Files are named using the pattern:
```
tmdb_{batch_type}_{batch_id}_{timestamp}.yaml
```
Example: `tmdb_missing_20240315_123456.yaml`

### Archiving
- After successful load, YAML files are moved to `data/yaml/archived/`
- Maintains a history of processed batches
- Prevents duplicate processing