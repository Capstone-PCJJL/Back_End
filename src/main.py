import sys
import time
from datetime import datetime
from tqdm import tqdm
from typing import Dict, List

from api.tmdb_client import TMDBClient
from models.movie import Movie
from storage.data_storage import DataStorage

def fetch_movies_by_year(tmdb_client: TMDBClient, year: int) -> List[Dict]:
    """
    Fetch all movies for a specific year.
    
    Args:
        tmdb_client: TMDB API client
        year: Year to fetch movies for
        
    Returns:
        List[Dict]: List of movie data
    """
    movies = []
    try:
        # Get first page
        first_page = tmdb_client.get_movies_by_year(year, 1)
        total_pages = min(first_page.get("total_pages", 1), 500)  # TMDB API max is 500
        movies.extend(first_page.get("results", []))

        # Get remaining pages
        for page in tqdm(range(2, total_pages + 1), desc=f"Fetching movies for {year}"):
            data = tmdb_client.get_movies_by_year(year, page)
            movies.extend(data.get("results", []))
            time.sleep(0.25)  # Respect API rate limits

    except Exception as e:
        print(f"Error fetching movies for year {year}: {str(e)}")
    
    return movies

def main():
    """Main function to orchestrate the movie data collection process."""
    try:
        # Initialize components
        print("Initializing components...")
        tmdb_client = TMDBClient()
        storage = DataStorage()

        # Fetch and save movie changes
        print("\nFetching recent movie changes...")
        changes = tmdb_client.get_movie_changes()
        storage.save_movie_changes(changes)

        # Fetch and save movies by year
        print("\nFetching movies by year...")
        current_year = datetime.now().year
        for year in range(1900, current_year + 1):
            print(f"\nProcessing year {year}...")
            
            # Fetch movies
            raw_movies = fetch_movies_by_year(tmdb_client, year)
            if not raw_movies:
                continue

            # Convert to Movie objects and save
            movie_objects = [Movie.from_api_data(data) for data in raw_movies]
            storage.save_movies_by_year([m.to_dict() for m in movie_objects], year)

    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\nAn unexpected error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 