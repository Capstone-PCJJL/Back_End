# Movie Database ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for movie data, combining information from MovieLens and TMDB APIs.

## Features

- Loads movie data from MovieLens dataset (ml-32m)
- Enriches data with TMDB API information
- Handles movie credits, videos, reviews, keywords, release dates, content ratings, and watch providers
- Supports incremental updates and specific movie updates
- Excludes adult content
- Comprehensive error handling and logging

## Prerequisites

- Python 3.8+
- MySQL 8.0+
- TMDB API key

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-name>
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

4. Download the MovieLens dataset:
   - Go to [MovieLens Datasets](https://grouplens.org/datasets/movielens/)
   - Download the "MovieLens 32M" dataset (ml-32m.zip)
   - Extract the zip file to the project root directory
   - The extracted directory should be named `ml-32m` and contain:
     - movies.csv
     - ratings.csv
     - tags.csv
     - links.csv

5. Create a `.env` file in the project root with your database and TMDB API credentials:
```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=your_database_name
TMDB_API_KEY=your_tmdb_api_key
```

## Usage

### Initial Load

To perform the initial load of movie data:

```bash
python -m src.etl.movie_etl init
```

Optional arguments:
- `--start-year`: Year to start loading from (default: 1900)
- `--end-year`: Year to end loading at (default: current year)

Example:
```bash
python -m src.etl.movie_etl init --start-year 2000 --end-year 2024
```

### Process Recent Changes

To process recent movie changes:

```bash
python -m src.etl.movie_etl changes
```

Optional arguments:
- `--days`: Number of days to look back for changes (default: 1)

Example:
```bash
python -m src.etl.movie_etl changes --days 7
```

### Update Specific Movie

To update a specific movie:

```bash
python -m src.etl.movie_etl update <tmdb_id>
```

Example:
```bash
python -m src.etl.movie_etl update 550
```

### Clear Database

To clear all data from the database:

```bash
python -m src.etl.movie_etl clear
```

Add `--force` to skip confirmation:
```bash
python -m src.etl.movie_etl clear --force
```

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

## Project Structure

```
.
├── config/                 # Configuration files
├── data/                   # Data storage
│   ├── movies/            # Movie data files
│   └── changes/           # Change tracking files
├── ml-32m/                # MovieLens dataset
│   ├── movies.csv
│   ├── ratings.csv
│   ├── tags.csv
│   └── links.csv
├── src/
│   ├── api/               # API clients
│   │   └── tmdb_client.py
│   ├── data/              # Data loaders
│   │   └── movielens_loader.py
│   ├── database/          # Database management
│   │   └── db_manager.py
│   └── etl/               # ETL pipeline
│       └── movie_etl.py
├── tests/                 # Test files
├── .env                   # Environment variables
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [MovieLens](https://grouplens.org/datasets/movielens/) for the movie dataset
- [TMDB](https://www.themoviedb.org/) for the movie metadata API