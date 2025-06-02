# Movie Data ETL System

A robust ETL (Extract, Transform, Load) system for movie data, combining information from MovieLens and TMDB (The Movie Database) APIs.

## Features

- **Dual Data Source Integration**
  - MovieLens dataset integration
  - TMDB API integration with automatic updates
  - Intelligent data merging and conflict resolution

- **Robust Data Processing**
  - YAML-based intermediate storage for data validation
  - Batch processing for efficient data loading
  - Automatic genre and relationship management
  - Comprehensive error handling and logging
  - Data validation and cleaning

- **Database Management**
  - MySQL database integration
  - Automatic schema management
  - Efficient bulk operations
  - Relationship tracking and updates

## Prerequisites

- Python 3.11+
- MySQL 8.0+
- TMDB API key
- MovieLens dataset

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd Back_End
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
Create a `.env` file in the project root with the following variables:
```env
TMDB_API_KEY=your_tmdb_api_key
SQL_HOST=localhost
SQL_PORT=3306
SQL_USER=your_db_user
SQL_PASS=your_db_password
SQL_DB=your_db_name
```

## Usage

### Data Processing Workflow

The system uses a two-step process for data loading:

1. **Data Extraction and Storage**
   - Data is fetched from TMDB API
   - Stored in YAML files for validation and inspection
   - Organized by batch type (initial, missing, changes)

2. **Data Loading**
   - YAML files are processed and loaded into the database
   - Batch processing for efficiency
   - Error handling and recovery
   - Data validation against schema
   - Updates existing records or adds new ones

### Data Validation

The system performs validation at multiple levels:

1. **Schema Validation**
   - Validates data structure against defined schema
   - Ensures required fields are present
   - Checks data types and formats

2. **Data Integrity**
   - Validates relationships between entities
   - Ensures referential integrity
   - Handles duplicate entries appropriately

3. **Batch Processing**
   - Validates data in batches for efficiency
   - Continues processing on non-critical errors
   - Logs validation issues for review

### Table Operations

Different commands handle database tables differently:

1. **Initial Load (`init`)**
   - Creates tables if they don't exist
   - Loads MovieLens data directly
   - Processes TMDB data through YAML files

2. **Missing Movies (`missing`)**
   - Updates existing movies
   - Adds new movies
   - Maintains relationships
   - Does NOT clear or drop tables

3. **Recent Changes (`changes`)**
   - Updates existing movies
   - Adds new movies
   - Maintains relationships
   - Does NOT clear or drop tables

4. **Clear Database (`clear`)**
   - Requires explicit `--force` flag
   - Removes all data while keeping structure
   - Requires user confirmation

### Initial Data Load

The initial data load process consists of three steps:

1. **Load MovieLens Data**
   ```bash
   # This loads the MovieLens CSV data directly into the database
   python -m src.etl.movie_etl init --load-movielens
   ```

2. **Fetch TMDB Data**
   ```bash
   # This fetches TMDB data from the latest MovieLens date to present
   # The script automatically determines the start date based on the latest MovieLens movie
   python -m src.etl.movie_etl init --fetch-tmdb
   ```

3. **Process TMDB Data**
   ```bash
   # This processes the YAML files containing TMDB data and loads them into the database
   python -m src.etl.yaml_processor --batch-type initial
   ```

Alternatively, you can run all steps in sequence:
```bash
# This will execute all three steps in order
python -m src.etl.movie_etl init --full
```

For custom year ranges in TMDB data:
```bash
# Specify custom year range for TMDB data
python -m src.etl.movie_etl init --fetch-tmdb --start-year 2020 --end-year 2023
```

### Incremental Updates

To update the database with new movies from TMDB:

```bash
# Step 1: Extract and store new movies
python -m src.etl.movie_etl missing --after-date 2023-01-01

# Step 2: Process YAML files and load into database
python -m src.etl.yaml_processor --batch-type missing
```

### Process Recent Changes

To process recent movie changes:

```bash
# Step 1: Extract and store changes
python -m src.etl.movie_etl changes --days 1

# Step 2: Process YAML files and load into database
python -m src.etl.yaml_processor --batch-type changes
```

### Process Specific YAML File

To process a specific YAML file:

```bash
python -m src.etl.yaml_processor --file data/movies/raw/initial_2023_20240101_120000.yaml
```

## Project Structure

```
src/
├── api/
│   ├── tmdb_client.py      # TMDB API integration
│   └── movielens_client.py # MovieLens data handling
├── database/
│   └── db_manager.py       # Database operations
├── etl/
│   ├── movie_etl.py        # Main ETL process
│   └── yaml_processor.py   # YAML file processing
└── utils/
    └── yaml_handler.py     # YAML file operations
```

## Data Model

The system maintains the following main entities:

- **Movies**: Core movie information
- **Genres**: Movie genres
- **People**: Cast and crew information
- **Credits**: Movie-people relationships
- **Keywords**: Movie keywords and tags

## Error Handling

The system includes comprehensive error handling:

- API rate limiting and retry logic
- Database transaction management
- Data validation and cleaning
- Detailed logging for debugging
- YAML-based error recovery

## Recent Improvements

- YAML-based intermediate storage for better data validation
- Separated data extraction and loading processes
- Enhanced error handling and recovery
- Improved batch processing
- Better data validation through schema enforcement

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Data Sources

### MovieLens Dataset (ml-32m)
- Contains 32 million ratings and 2 million tag applications
- Applied to 87,585 movies by 200,948 users
- Collected from January 1995 to October 2023
- Includes movie metadata, user ratings, and tags

### TMDB API
- Provides additional movie metadata
- Includes credits, videos, reviews, keywords
- Contains release dates, content ratings, and watch providers
- Used to enrich the MovieLens data