# Movie Data Collection and Processing System

A Python-based system for collecting and processing movie data from The Movie Database (TMDB) API. This system fetches movie information, processes it, and stores it in an organized manner for further analysis.

## Project Structure

```
Back_End/
├── src/
│   ├── api/
│   │   └── tmdb_client.py      # TMDB API client for data fetching
│   ├── models/
│   │   └── movie.py           # Movie data model using dataclasses
│   ├── storage/
│   │   └── data_storage.py    # Data storage handler for saving files
│   └── main.py                # Main script orchestrating the process
├── data/
│   ├── movies/                # YAML files containing movie data by year
│   └── changes/               # YAML files containing movie changes
├── config/
│   └── .env.example          # Example environment variables file
├── requirements.txt          # Python dependencies
└── README.md                # Project documentation
```

## Features

- Fetches movie data from TMDB API (1900 to present)
- Processes and stores all data in YAML format
- Handles API rate limiting and error recovery
- Provides progress tracking with loading bars
- Organizes data by year
- Includes direct image URLs in movie data
- Properly handles missing image data

## Prerequisites

- Python 3.8 or higher
- TMDB API credentials (API Key and Bearer Token)

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd Back_End
```

2. Create and activate a virtual environment:
```bash
# On macOS/Linux
python -m venv venv
source venv/bin/activate

# On Windows
python -m venv venv
venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
   - Copy `config/.env.example` to `config/.env`
   - Add your TMDB API credentials:
```
TMDB_API_KEY=your_api_key_here
TMDB_BEARER_TOKEN=your_bearer_token_here
```

## Usage

Run the main script to start collecting movie data:
```bash
python src/main.py
```

The script will:
1. Create necessary data directories
2. Fetch and save recent movie changes
3. Fetch movies year by year (1900 to present)
4. Save movies in YAML format with image URLs

## Data Organization

### Movies
- Stored in `data/movies/` directory
- Files named `movies_YYYY.yaml`
- Contains movie details including:
  - Title, release date, overview
  - Popularity and vote statistics
  - Genre IDs and language information
  - Poster and backdrop paths (null if not available)
  - Direct URLs to poster and backdrop images (null if not available)

### Changes
- Stored in `data/changes/` directory
- Files named `changes_YYYYMMDD_HHMMSS.yaml`
- Contains recent movie changes from TMDB

## Development

### Code Style
- Follow PEP 8 guidelines
- Use type hints for better code understanding
- Document all functions and classes

### Error Handling
- All API calls include error handling
- Rate limiting implemented (0.25s between requests)
- Graceful error recovery and logging

### Testing
- Use pytest for testing
- Run tests with:
```bash
pytest tests/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Add your license information here]