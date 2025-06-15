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
