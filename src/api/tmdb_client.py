import os
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TMDBClient:
    """Enhanced client for interacting with The Movie Database (TMDB) API."""
    
    def __init__(self):
        """Initialize the TMDB client with API credentials."""
        load_dotenv()
        self.api_key = os.getenv("API_KEY")
        self.bearer_token = os.getenv("TMDB_BEARER_TOKEN")
        self.base_url = "https://api.themoviedb.org/3"
        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.bearer_token}"
        }
        
        if not self.api_key or not self.bearer_token:
            raise ValueError("TMDB API credentials not found in environment variables")

    def _make_request(self, endpoint: str, params: Dict[str, Any] = None, use_api_key: bool = False) -> Dict[str, Any]:
        """
        Make a request to the TMDB API.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            use_api_key: Whether to use API key authentication
            
        Returns:
            Dict[str, Any]: API response
        """
        if params is None:
            params = {}
            
        if use_api_key:
            params['api_key'] = self.api_key
            
        url = f"{self.base_url}/{endpoint}"
        
        max_retries = 3
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url, 
                    params=params,
                    timeout=10,  # Add timeout
                    verify=True  # Enable SSL verification
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.SSLError as e:
                logger.error(f"SSL Error on attempt {attempt + 1}: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed on attempt {attempt + 1}: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(retry_delay * (attempt + 1))  # Exponential backoff

    def get_all_movies(self, start_year: int = 1900, end_year: int = None) -> List[Dict[str, Any]]:
        """
        Get all movies from TMDB within a year range.
        
        Args:
            start_year: Start year to fetch movies from
            end_year: End year to fetch movies to (defaults to current year)
            
        Returns:
            List[Dict[str, Any]]: List of movie data
        """
        if end_year is None:
            end_year = datetime.now().year
            
        all_movies = []
        for year in range(start_year, end_year + 1):
            logger.info(f"Fetching movies for year {year}")
            movies = self.get_movies_by_year(year)
            all_movies.extend(movies)
            time.sleep(0.25)  # Rate limiting
            
        return all_movies

    def get_movies_by_year(self, year: int) -> List[Dict[str, Any]]:
        """
        Get movies released in a specific year from TMDB.
        
        Args:
            year: Year to get movies for
            
        Returns:
            List[Dict[str, Any]]: List of movie data
        """
        try:
            # Get movies released in the year
            movies = []
            page = 1
            total_pages = 1
            
            while page <= total_pages:
                response = self._make_request(
                    'discover/movie',
                    params={
                        'primary_release_year': year,
                        'page': page,
                        'sort_by': 'popularity.desc',
                        'include_adult': False,  # Exclude adult content
                        'language': 'en-US'  # English language
                    }
                )
                
                if not response:
                    break
                    
                movies.extend(response.get('results', []))
                total_pages = response.get('total_pages', 1)
                page += 1
                
                # Rate limiting
                time.sleep(0.25)
            
            return movies
            
        except Exception as e:
            logger.error(f"Error getting movies for year {year}: {str(e)}")
            return []

    def get_movie_details(self, movie_id: int, append_to_response: str = None) -> Dict:
        """
        Fetch detailed information for a specific movie.
        
        Args:
            movie_id: The TMDB ID of the movie
            append_to_response: Additional data to include (credits,images,videos,etc.)
            
        Returns:
            Dict: JSON response from the API
        """
        params = {}
        if append_to_response:
            params["append_to_response"] = append_to_response
        return self._make_request(f"movie/{movie_id}", params, use_api_key=True)

    def get_movie_changes(self, start_date: str = None, end_date: str = None, page: int = 1) -> Dict:
        """
        Fetch movie changes between two dates.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            page: The page number to fetch
            
        Returns:
            Dict: JSON response from the API
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
            
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "page": page
        }
        return self._make_request("movie/changes", params)

    def get_movie_images(self, movie_id: int, include_image_language: str = None) -> Dict:
        """
        Fetch images for a specific movie.
        
        Args:
            movie_id: The TMDB ID of the movie
            include_image_language: Filter images by language (en,null,etc.)
            
        Returns:
            Dict: JSON response from the API
        """
        params = {}
        if include_image_language:
            params["include_image_language"] = include_image_language
        return self._make_request(f"movie/{movie_id}/images", params)

    def get_movie_credits(self, movie_id: int) -> Dict:
        """
        Fetch cast and crew information for a movie.
        
        Args:
            movie_id: The TMDB ID of the movie
            
        Returns:
            Dict: JSON response from the API
        """
        return self._make_request(f"movie/{movie_id}/credits", use_api_key=True)

    def get_movie_videos(self, movie_id: int) -> Dict:
        """
        Fetch videos (trailers, teasers, etc.) for a movie.
        
        Args:
            movie_id: The TMDB ID of the movie
            
        Returns:
            Dict: JSON response from the API
        """
        return self._make_request(f"movie/{movie_id}/videos", use_api_key=True)

    def get_movie_reviews(self, movie_id: int, page: int = 1) -> Dict:
        """
        Fetch user reviews for a movie.
        
        Args:
            movie_id: The TMDB ID of the movie
            page: The page number to fetch
            
        Returns:
            Dict: JSON response from the API
        """
        params = {"page": page}
        return self._make_request(f"movie/{movie_id}/reviews", params, use_api_key=True)

    def get_movie_recommendations(self, movie_id: int, page: int = 1) -> Dict:
        """
        Fetch movie recommendations based on a movie.
        
        Args:
            movie_id: The TMDB ID of the movie
            page: The page number to fetch
            
        Returns:
            Dict: JSON response from the API
        """
        params = {"page": page}
        return self._make_request(f"movie/{movie_id}/recommendations", params, use_api_key=True)

    def get_movie_similar(self, movie_id: int, page: int = 1) -> Dict:
        """
        Fetch similar movies.
        
        Args:
            movie_id: The TMDB ID of the movie
            page: The page number to fetch
            
        Returns:
            Dict: JSON response from the API
        """
        params = {"page": page}
        return self._make_request(f"movie/{movie_id}/similar", params, use_api_key=True)

    def get_movie_keywords(self, movie_id: int) -> Dict:
        """
        Fetch keywords associated with a movie.
        
        Args:
            movie_id: The TMDB ID of the movie
            
        Returns:
            Dict: JSON response from the API
        """
        return self._make_request(f"movie/{movie_id}/keywords", use_api_key=True)

    def get_movie_external_ids(self, movie_id: int) -> Dict:
        """
        Fetch external IDs (IMDB, Facebook, Instagram, Twitter) for a movie.
        
        Args:
            movie_id: The TMDB ID of the movie
            
        Returns:
            Dict: JSON response from the API
        """
        return self._make_request(f"movie/{movie_id}/external_ids", use_api_key=True)

    def get_movie_watch_providers(self, movie_id: int) -> Dict:
        """
        Fetch watch providers (streaming services) for a movie.
        
        Args:
            movie_id: The TMDB ID of the movie
            
        Returns:
            Dict: JSON response from the API
        """
        return self._make_request(f"movie/{movie_id}/watch/providers", use_api_key=True)

    def get_movie_translations(self, movie_id: int) -> Dict:
        """
        Fetch available translations for a movie.
        
        Args:
            movie_id: The TMDB ID of the movie
            
        Returns:
            Dict: JSON response from the API
        """
        return self._make_request(f"movie/{movie_id}/translations", use_api_key=True)

    def get_movie_lists(self, movie_id: int, page: int = 1) -> Dict:
        """
        Fetch lists that contain the movie.
        
        Args:
            movie_id: The TMDB ID of the movie
            page: The page number to fetch
            
        Returns:
            Dict: JSON response from the API
        """
        params = {"page": page}
        return self._make_request(f"movie/{movie_id}/lists", params, use_api_key=True)

    def get_movie_release_dates(self, movie_id: int) -> Dict:
        """
        Fetch release dates for a movie in different countries.
        
        Args:
            movie_id: The TMDB ID of the movie
            
        Returns:
            Dict: JSON response from the API
        """
        return self._make_request(f"movie/{movie_id}/release_dates", use_api_key=True)

    def get_movie_content_ratings(self, movie_id: int) -> Dict:
        """
        Fetch content ratings for a movie in different countries.
        
        Args:
            movie_id: The TMDB ID of the movie
            
        Returns:
            Dict: JSON response from the API
        """
        return self._make_request(f"movie/{movie_id}/content_ratings", use_api_key=True) 