import os
import logging
import time
from typing import List, Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
# load .senv
from dotenv import load_dotenv
import threading
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/tmdb_api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TMDBClient:
    """Client for interacting with TMDB API."""
    
    def __init__(self):
        """Initialize TMDB client with API key."""
        self.api_key = os.getenv('API_KEY')
        self.base_url = os.getenv('BASE_URL', 'https://api.themoviedb.org/3')
        self.bearer_token = os.getenv('TMDB_BEARER_TOKEN')
        
        if not self.api_key:
            raise ValueError("API_KEY environment variable not set")
        if not self.bearer_token:
            raise ValueError("TMDB_BEARER_TOKEN environment variable not set")
        
        # Create a session with connection pooling and retries
        self.session = requests.Session()
        
        # Configure retry strategy with exponential backoff
        retry_strategy = Retry(
            total=3,  # Reduced retries
            backoff_factor=0.1,  # Reduced backoff time
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        # Mount the adapter with retry strategy and optimized pool
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,     # Reduced initial pool size
            pool_maxsize=100,        # Keep max connections high
            pool_block=True          # Block when pool is full
        )
        self.session.mount("https://", adapter)
        
        # Set headers
        self.session.headers.update({
            'Authorization': f'Bearer {self.bearer_token}',
            'accept': 'application/json'
        })
        
        # Rate limiting
        self.request_times = []
        self.max_requests_per_second = 40  # Increased rate limit
        
        # Thread-safe caching
        self.request_cache = {}
        self.cache_ttl = 3600  # Cache TTL in seconds
        self.cache_lock = threading.Lock()
        self.last_cache_cleanup = time.time()
        self.cache_cleanup_interval = 300  # Clean cache every 5 minutes
        
        # Test connection
        try:
            test_response = self._make_request('configuration')
            if not test_response:
                raise ValueError("Failed to connect to TMDB API")
            logger.info("Successfully connected to TMDB API")
        except Exception as e:
            logger.error(f"Failed to initialize TMDB client: {str(e)}")
            raise

    def _cleanup_cache(self):
        """Clean up expired cache entries."""
        now = time.time()
        if now - self.last_cache_cleanup > self.cache_cleanup_interval:
            with self.cache_lock:
                self.request_cache = {k: v for k, v in self.request_cache.items() 
                                    if now - v[0] < self.cache_ttl}
                self.last_cache_cleanup = now

    def _get_from_cache(self, cache_key: str) -> Optional[Dict]:
        """Get data from cache in a thread-safe way."""
        with self.cache_lock:
            if cache_key in self.request_cache:
                cache_time, cache_data = self.request_cache[cache_key]
                if time.time() - cache_time < self.cache_ttl:
                    return cache_data
        return None

    def _add_to_cache(self, cache_key: str, data: Dict):
        """Add data to cache in a thread-safe way."""
        with self.cache_lock:
            self.request_cache[cache_key] = (time.time(), data)

    def _rate_limit(self):
        """Implement rate limiting."""
        now = time.time()
        # Remove requests older than 1 second
        self.request_times = [t for t in self.request_times if now - t < 1]
        
        # If we've made too many requests in the last second, wait
        if len(self.request_times) >= self.max_requests_per_second:
            sleep_time = 1 - (now - self.request_times[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
            # Clean up old requests again
            self.request_times = [t for t in self.request_times if now - t < 1]
        
        # Add current request
        self.request_times.append(now)

    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make a request to the TMDB API with rate limiting and caching."""
        try:
            # Check cache first
            cache_key = f"{endpoint}:{str(params)}"
            cached_data = self._get_from_cache(cache_key)
            if cached_data:
                return cached_data
            
            self._rate_limit()  # Apply rate limiting
            
            url = f"{self.base_url}/{endpoint}"
            response = self.session.get(url, params=params, timeout=5)  # Added timeout
            response.raise_for_status()
            data = response.json()
            
            # Cache the response
            self._add_to_cache(cache_key, data)
            
            # Periodically clean up cache
            self._cleanup_cache()
            
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            return None
    
    def get_movie_ids(self, since_id: int = None, test_year: int = None) -> List[int]:
        """Get all movie IDs from TMDB, year by year."""
        movie_ids = set()  # Use set to avoid duplicates
        max_pages = 300  # Reduced from 500 to 300
        
        try:
            if test_year:
                # If test_year is provided, only process that year
                years_to_process = [test_year]
                logger.info(f"Test mode: Processing only year {test_year}")
            else:
                # Get earliest and latest years
                earliest_params = {
                    'sort_by': 'release_date.asc',
                    'page': 1,
                    'include_adult': False,
                    'include_video': False
                }
                earliest_data = self._make_request('discover/movie', earliest_params)
                earliest_year = datetime.strptime(earliest_data['results'][0]['release_date'], '%Y-%m-%d').year
                
                # Set latest year to 2023 for initial data load
                latest_year = 2023
                
                years_to_process = range(earliest_year, latest_year + 1)
                logger.info(f"Fetching movies from {earliest_year} to {latest_year}")
            
            # Get movies year by year
            for year in years_to_process:
                logger.info(f"Fetching movies from year {year}")
                
                # Get total pages for this year
                year_params = {
                    'primary_release_year': year,
                    'sort_by': 'popularity.desc',  # Only use popularity sorting
                    'page': 1,
                    'include_adult': False,
                    'include_video': False
                }
                year_data = self._make_request('discover/movie', year_params)
                total_pages = min(year_data.get('total_pages', 0), max_pages)
                
                # Create progress bar for this year
                with tqdm(total=total_pages, desc=f"Pages for {year}", leave=False) as pbar:
                    page = 1
                    while page <= total_pages:
                        try:
                            params = {
                                'primary_release_year': year,
                                'sort_by': 'popularity.desc',
                                'page': page,
                                'include_adult': False,
                                'include_video': False
                            }
                            
                            data = self._make_request('discover/movie', params)
                            if not data or not data.get('results'):
                                break
                            
                            # Extract movie IDs
                            for movie in data.get('results', []):
                                movie_id = movie.get('id')
                                if movie_id:
                                    if since_id and movie_id <= since_id:
                                        continue
                                    movie_ids.add(movie_id)
                            
                            page += 1
                            pbar.update(1)
                            
                        except Exception as e:
                            logger.error(f"Error getting movies for year {year}: {str(e)}")
                            break
        
        except Exception as e:
            logger.error(f"Error getting year range: {str(e)}")
            return list(movie_ids)
        
        # Convert set to sorted list
        movie_ids = sorted(list(movie_ids))
        logger.info(f"Found {len(movie_ids)} unique movies to process")
        return movie_ids

    def _fetch_movies_for_year(self, year: int, sort_by: str, since_id: int = None) -> set:
        """Fetch movies for a specific year and sort criteria."""
        movie_ids = set()
        max_pages = 300  # Reduced from 500 to 300
        page = 1
        
        try:
            # Get total pages for this year and sort criteria
            year_params = {
                'primary_release_year': year,
                'sort_by': sort_by,
                'page': 1,
                'include_adult': False,
                'include_video': False
            }
            year_data = self._make_request('discover/movie', year_params)
            total_pages = min(year_data.get('total_pages', 0), max_pages)
            
            # Create progress bar for this year and sort criteria
            with tqdm(total=total_pages, desc=f"Pages for {year} ({sort_by})", leave=False) as pbar:
                while page <= total_pages:
                    try:
                        params = {
                            'primary_release_year': year,
                            'sort_by': sort_by,
                            'page': page,
                            'include_adult': False,
                            'include_video': False
                        }
                        
                        data = self._make_request('discover/movie', params)
                        if not data or not data.get('results'):
                            break
                        
                        # Extract movie IDs
                        for movie in data.get('results', []):
                            movie_id = movie.get('id')
                            if movie_id:
                                if since_id and movie_id <= since_id:
                                    continue
                                movie_ids.add(movie_id)
                        
                        page += 1
                        pbar.update(1)
                        
                        # Reduced delay between requests
                        time.sleep(0.01)  # 10ms delay between requests
                        
                    except Exception as e:
                        logger.error(f"Error getting movies for year {year} with sort {sort_by}: {str(e)}")
                        break
        
        except Exception as e:
            logger.error(f"Error getting data for year {year} with sort {sort_by}: {str(e)}")
        
        return movie_ids
    
    @lru_cache(maxsize=10000)  # increased cache size
    def get_movie_details(self, movie_id: int) -> Optional[Dict]:
        """Get detailed information about a movie."""
        try:
            return self._make_request(f'movie/{movie_id}')
        except Exception as e:
            logger.error(f"Error getting movie details for ID {movie_id}: {str(e)}")
            return None
    
    @lru_cache(maxsize=10000)  # increased cache size
    def get_movie_credits(self, movie_id: int) -> Optional[Dict]:
        """Get cast and crew information for a movie."""
        try:
            return self._make_request(f'movie/{movie_id}/credits')
        except Exception as e:
            logger.error(f"Error getting credits for movie ID {movie_id}: {str(e)}")
            return None

    @lru_cache(maxsize=10000)  # increased cache size
    def get_person(self, person_id: int) -> Optional[Dict]:
        """Get detailed information about a person."""
        try:
            return self._make_request(f'person/{person_id}')
        except Exception as e:
            logger.error(f"Error getting person details for ID {person_id}: {str(e)}")
            return None

    def search_movie(self, query: str) -> List[Dict]:
        """Search for movies by title or ID.
        
        Args:
            query: Movie title or ID to search for
            
        Returns:
            List of movie results from TMDB
        """
        try:
            # If query is a movie ID, get that specific movie
            if query.isdigit():
                movie_id = int(query)
                movie_data = self.get_movie_details(movie_id)
                return [movie_data] if movie_data else []

            # Otherwise search by title
            params = {
                'query': query,
                'include_adult': False,
                'language': 'en-US',
                'page': 1
            }
            
            response = self._make_request('search/movie', params)
            if not response or 'results' not in response:
                return []
                
            return response['results']
            
        except Exception as e:
            logger.error(f"Error searching for movie '{query}': {str(e)}")
            return []

    def get_movies_since_date(self, start_date: datetime) -> List[Dict]:
        """Get movies released since a specific date.
        
        Args:
            start_date: The date to start searching from
            
        Returns:
            List of movie data from TMDB
        """
        try:
            movies = []
            page = 1
            total_pages = 1
            
            # Format date for API
            start_date_str = start_date.strftime('%Y-%m-%d')
            
            while page <= total_pages:
                params = {
                    'primary_release_date.gte': start_date_str,
                    'sort_by': 'release_date.desc',
                    'page': page,
                    'include_adult': False,
                    'include_video': False
                }
                
                response = self._make_request('discover/movie', params)
                if not response or 'results' not in response:
                    break
                    
                movies.extend(response['results'])
                total_pages = min(response.get('total_pages', 1), 20)  # Limit to 20 pages
                page += 1
                
                # Add a small delay between requests
                time.sleep(0.1)
            
            return movies
            
        except Exception as e:
            logger.error(f"Error getting movies since {start_date}: {str(e)}")
            return [] 