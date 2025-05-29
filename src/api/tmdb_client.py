import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

class TMDBClient:
    """Client for interacting with The Movie Database (TMDB) API."""
    
    def __init__(self):
        """Initialize the TMDB client with API credentials."""
        self.api_key = os.getenv("API_KEY")
        self.bearer_token = os.getenv("TMDB_BEARER_TOKEN")
        self.base_url = "https://api.themoviedb.org/3"
        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.bearer_token}"
        }
        
        if not self.api_key or not self.bearer_token:
            raise ValueError("TMDB API credentials not found in environment variables")

    def get_movies_by_year(self, year: int, page: int = 1) -> dict:
        """
        Fetch movies for a specific year.
        
        Args:
            year: The year to fetch movies for
            page: The page number to fetch
            
        Returns:
            dict: JSON response from the API
        """
        url = f"{self.base_url}/discover/movie"
        params = {
            "api_key": self.api_key,
            "sort_by": "popularity.desc",
            "page": page,
            "primary_release_year": year
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_movie_changes(self, start_date: str = None, end_date: str = None, page: int = 1) -> dict:
        """
        Fetch movie changes between two dates.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            page: The page number to fetch
            
        Returns:
            dict: JSON response from the API
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
            
        url = f"{self.base_url}/movie/changes"
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "page": page
        }
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_movie_images(self, movie_id: int) -> dict:
        """
        Fetch images for a specific movie.
        
        Args:
            movie_id: The TMDB ID of the movie
            
        Returns:
            dict: JSON response from the API
        """
        url = f"{self.base_url}/movie/{movie_id}/images"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_movie_details(self, movie_id: int) -> dict:
        """
        Fetch detailed information for a specific movie.
        
        Args:
            movie_id: The TMDB ID of the movie
            
        Returns:
            dict: JSON response from the API
        """
        url = f"{self.base_url}/movie/{movie_id}"
        params = {"api_key": self.api_key}
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json() 