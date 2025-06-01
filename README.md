# Movie ETL Pipeline

This project provides an ETL (Extract, Transform, Load) pipeline for movie data, integrating MovieLens and TMDB datasets.

## Overview

- **Initial Load**: Loads all MovieLens data into the database without TMDB enrichment.
- **Missing Movies**: Fetches movies from TMDB that are not present in the current dataset, optionally after a specific date.
- **Change Tracking**: Processes recent movie changes from TMDB.

## Setup

1. **Environment**: Ensure you have Python 3.8+ installed.
2. **Dependencies**: Install required packages using `pip install -r requirements.txt`.
3. **Configuration**: Update `config/config.yaml` with your TMDB API key and database credentials.

## Usage

### Initial Load

To perform the initial load of MovieLens data:

```bash
python -m src.etl.movie_etl init
```

### Missing Movies

To fetch missing movies from TMDB after the latest movie in the database:

```bash
python -m src.etl.movie_etl missing
```

### Process Recent Changes

To process recent movie changes from TMDB:

```bash
python -m src.etl.movie_etl changes --days 1
```

### Update a Specific Movie

To update a specific movie by its TMDB ID:

```bash
python -m src.etl.movie_etl update <tmdb_id>
```

### Clear Database

To clear all data from the database (use with caution):

```bash
python -m src.etl.movie_etl clear --force
```

## Project Structure

- `src/etl/movie_etl.py`: Main ETL pipeline logic.
- `src/data/movielens_loader.py`: Handles loading MovieLens data.
- `src/api/tmdb_client.py`: Manages TMDB API interactions.
- `src/database/db_manager.py`: Database operations and models.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

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

## Acknowledgments

- [MovieLens](https://grouplens.org/datasets/movielens/) for the movie dataset
- [TMDB](https://www.themoviedb.org/) for the movie metadata API