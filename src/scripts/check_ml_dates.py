import pandas as pd
import os
from datetime import datetime
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_year_from_title(title):
    """Extract year from movie title in format 'Movie Name (YYYY)'."""
    try:
        year = title.strip()[-5:-1]  # Get the year from the last 4 digits before the closing parenthesis
        return int(year)
    except (ValueError, IndexError):
        return None

def get_latest_movie_date():
    """Get the latest movie release date from the MovieLens dataset."""
    try:
        # Use the correct absolute path
        movies_path = '/Users/jeevanparmar/Uni/Capstone/Back_End/ml-32m/movies.csv'
        
        if not os.path.exists(movies_path):
            logger.error(f"Movies file not found at {movies_path}")
            return None

        logger.info(f"Reading movies file from: {movies_path}")

        # Read the CSV file
        df = pd.read_csv(movies_path)
        
        # Extract year from title
        df['year'] = df['title'].apply(extract_year_from_title)
        
        # Get the latest year
        latest_year = df['year'].max()
        
        # Get movies from the latest year
        latest_movies = df[df['year'] == latest_year]
        
        logger.info(f"Latest movie year in MovieLens dataset: {latest_year}")
        logger.info(f"Number of movies from {latest_year}: {len(latest_movies)}")
        logger.info("\nSample of latest movies:")
        for _, movie in latest_movies.head().iterrows():
            logger.info(f"- {movie['title']}")
        
        return latest_year

    except Exception as e:
        logger.error(f"Error reading MovieLens data: {str(e)}")
        return None

if __name__ == "__main__":
    latest_year = get_latest_movie_date()
    if latest_year:
        logger.info(f"\nLatest movie year in MovieLens dataset: {latest_year}") 