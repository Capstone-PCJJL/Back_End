import logging
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Set
import pandas as pd
from tqdm import tqdm
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from src.api.tmdb_client import TMDBClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TMDBETL:
    def __init__(self):
        """Initialize TMDB ETL process."""
        self.client = TMDBClient()
        self.csv_dir = Path('data/csv')
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize DataFrames
        self.movies_df = pd.DataFrame()
        self.credits_df = pd.DataFrame()
        self.people_df = pd.DataFrame()
        self.genres_df = pd.DataFrame()
        
        # Initialize error tracking
        self.error_stats = {
            'not_found': set(),  # 404 errors
            'timeout': set(),    # Timeout errors
            'api_error': set(),  # Other API errors
            'processing_error': set(),  # Data processing errors
            'removed_movies': set(),  # Movies that were found but later removed
            'validation_error': set(),  # Movies that failed validation
            'rate_limit': set(),  # Movies that hit rate limits
            'no_credits': set(),  # Movies without credits
            'no_actors': set()    # Movies without actors
        }
        
        # Set up signal handler
        signal.signal(signal.SIGINT, self._signal_handler)
        self.interrupted = False
        self.person_cache = {}  # Cache for person data
        
        # Initialize retry configuration
        self.max_retries = 3
        self.retry_delay = 1  # seconds
        self.backoff_factor = 2
        
        # Rate limiting configuration
        self.rate_limit_delay = 0.25  # 250ms between requests (4 requests per second)
        self.last_request_time = 0
        self.rate_limit_retry_delay = 10  # 10 seconds when hitting rate limit

    def _signal_handler(self, signum, frame):
        """Handle interrupt signal."""
        logger.info("Process interrupted by user. Exiting gracefully.")
        self.interrupted = True
        sys.exit(0)

    def _wait_for_rate_limit(self):
        """Implement rate limiting between requests."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last_request)
        self.last_request_time = time.time()

    def _validate_movie_data(self, movie_data: Dict) -> bool:
        """Validate movie data before processing."""
        try:
            # Basic required fields
            required_fields = ['id', 'title', 'release_date']
            if not all(field in movie_data for field in required_fields):
                logger.warning(f"Movie {movie_data.get('id', 'unknown')} missing required fields")
                return False
            
            # Validate release date
            try:
                pd.to_datetime(movie_data['release_date'])
            except (ValueError, TypeError):
                logger.warning(f"Movie {movie_data['id']} has invalid release date: {movie_data['release_date']}")
                return False
            
            # Validate numeric fields
            numeric_fields = ['runtime', 'vote_average', 'vote_count', 'popularity', 'budget', 'revenue']
            for field in numeric_fields:
                if field in movie_data and movie_data[field] is not None:
                    try:
                        float(movie_data[field])
                    except (ValueError, TypeError):
                        logger.warning(f"Movie {movie_data['id']} has invalid {field}: {movie_data[field]}")
                        return False
            
            return True
        except Exception as e:
            logger.error(f"Error validating movie data: {str(e)}")
            return False

    def _process_movie(self, movie_id: int) -> Optional[Dict]:
        """Process a single movie and its related data."""
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                # Get movie details with caching
                movie_data = self.client.get_movie_details(movie_id)
                if not movie_data:
                    self.error_stats['not_found'].add(movie_id)
                    return None

                # Validate movie data
                if not self._validate_movie_data(movie_data):
                    self.error_stats['validation_error'].add(movie_id)
                    return None

                # Get credits with caching
                credits_data = self.client.get_movie_credits(movie_id)
                if not credits_data:
                    logger.warning(f"No credits found for movie {movie_id}")
                    credits_data = {'cast': [], 'crew': []}

                # Process cast and directors in parallel
                people_data = []
                with ThreadPoolExecutor(max_workers=20) as executor:
                    # Submit all person detail requests
                    person_futures = {}
                    
                    # Process cast and directors together with deduplication
                    all_people = []
                    # Get cast (actors) - top 8
                    all_people.extend(credits_data.get('cast', [])[:8])
                    # Get main director from crew
                    directors = [person for person in credits_data.get('crew', []) 
                               if person.get('job') == 'Director'][:1]  # Only get the first director
                    all_people.extend(directors)
                    
                    # Deduplicate people and filter out already cached
                    seen_ids = set()
                    for person in all_people:
                        person_id = person.get('id')
                        if person_id and person_id not in self.person_cache and person_id not in seen_ids:
                            seen_ids.add(person_id)
                            person_futures[executor.submit(self.client.get_person, person_id)] = person_id
                    
                    # Process results as they complete
                    for future in as_completed(person_futures):
                        try:
                            person_data = future.result()
                            if person_data:
                                self.person_cache[person_futures[future]] = person_data
                                people_data.append(person_data)
                        except requests.exceptions.Timeout:
                            self.error_stats['timeout'].add(person_futures[future])
                            logger.error(f"Timeout getting person details for ID {person_futures[future]}")
                        except requests.exceptions.RequestException as e:
                            self.error_stats['api_error'].add(person_futures[future])
                            logger.error(f"API error getting person details for ID {person_futures[future]}: {str(e)}")
                        except Exception as e:
                            self.error_stats['processing_error'].add(person_futures[future])
                            logger.error(f"Error processing person details for ID {person_futures[future]}: {str(e)}")

                # Create movie record
                movie_record = {
                    'id': movie_data['id'],
                    'title': movie_data['title'],
                    'original_title': movie_data['original_title'],
                    'overview': movie_data['overview'],
                    'release_date': movie_data['release_date'],
                    'runtime': movie_data['runtime'],
                    'status': movie_data['status'],
                    'vote_average': movie_data['vote_average'],
                    'vote_count': movie_data['vote_count'],
                    'popularity': movie_data['popularity'],
                    'poster_path': movie_data['poster_path'],
                    'backdrop_path': movie_data['backdrop_path'],
                    'budget': movie_data['budget'],
                    'revenue': movie_data['revenue']
                }

                # Create credits records
                credits_records = []
                # Add cast (actors) - top 8
                for person in credits_data.get('cast', [])[:8]:
                    if person.get('id'):
                        credits_records.append({
                            'movie_id': movie_id,
                            'person_id': person['id'],
                            'credit_type': 'cast',
                            'character_name': person.get('character'),
                            'credit_order': person.get('order')
                        })

                # Add main director
                directors = [person for person in credits_data.get('crew', [])
                            if person.get('job') == 'Director'][:1]  # Only get the first director
                for person in directors:
                    if person.get('id'):
                        credits_records.append({
                            'movie_id': movie_id,
                            'person_id': person['id'],
                            'credit_type': 'crew',
                            'department': 'Directing',
                            'job': 'Director'
                        })

                # Create people records
                people_records = []
                for person in people_data:
                    people_records.append({
                        'id': person['id'],
                        'name': person['name'],
                        'profile_path': person.get('profile_path'),
                        'gender': person.get('gender'),
                        'known_for_department': person.get('known_for_department')
                    })

                # Create genres records
                genres_records = []
                for genre in movie_data.get('genres', []):
                    genres_records.append({
                        'movie_id': movie_id,
                        'genre_name': genre['name']
                    })

                return {
                    'movie': movie_record,
                    'credits': credits_records,
                    'people': people_records,
                    'genres': genres_records
                }

            except requests.exceptions.Timeout:
                retry_count += 1
                if retry_count < self.max_retries:
                    wait_time = self.retry_delay * (self.backoff_factor ** (retry_count - 1))
                    logger.warning(f"Timeout processing movie {movie_id}, retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                self.error_stats['timeout'].add(movie_id)
                logger.error(f"Timeout processing movie {movie_id} after {self.max_retries} retries")
                return None
            except requests.exceptions.RequestException as e:
                if e.response and e.response.status_code == 404:
                    self.error_stats['not_found'].add(movie_id)
                    logger.error(f"Movie {movie_id} not found in TMDB")
                    return None
                retry_count += 1
                if retry_count < self.max_retries:
                    wait_time = self.retry_delay * (self.backoff_factor ** (retry_count - 1))
                    logger.warning(f"API error processing movie {movie_id}, retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                self.error_stats['api_error'].add(movie_id)
                logger.error(f"API error processing movie {movie_id} after {self.max_retries} retries: {str(e)}")
                return None
            except Exception as e:
                self.error_stats['processing_error'].add(movie_id)
                logger.error(f"Error processing movie {movie_id}: {str(e)}")
                return None

    def _append_to_dataframes(self, data: Dict[str, Any]):
        """Append processed data to DataFrames."""
        if not data:
            return

        try:
            # Append movie
            if data.get('movie'):
                self.movies_df = pd.concat([self.movies_df, pd.DataFrame([data['movie']])], ignore_index=True)

            # Append credits
            if data.get('credits'):
                self.credits_df = pd.concat([self.credits_df, pd.DataFrame(data['credits'])], ignore_index=True)

            # Append people
            if data.get('people'):
                self.people_df = pd.concat([self.people_df, pd.DataFrame(data['people'])], ignore_index=True)

            # Append genres
            if data.get('genres'):
                self.genres_df = pd.concat([self.genres_df, pd.DataFrame(data['genres'])], ignore_index=True)

        except Exception as e:
            logger.error(f"Error appending data to DataFrames: {str(e)}")
            raise

    def _save_dataframes(self):
        """Save all DataFrames to CSV files with consistent formatting."""
        logger.info("Saving data to CSV files...")
        
        # Common CSV writing parameters for consistency
        csv_params = {
            'index': False,
            'encoding': 'utf-8',
            'quoting': 1,  # QUOTE_ALL - quote all fields
            'quotechar': '"',
            'escapechar': '\\',
            'doublequote': True,  # Double up quotes to escape them
            'lineterminator': '\n',  # Unix-style line endings
            'na_rep': '',  # Empty string for NA values
            'float_format': '%.4f',  # Consistent float formatting with 4 decimal places
            'date_format': '%Y-%m-%d'  # Consistent date formatting
        }
        
        try:
            # Save movies
            if not self.movies_df.empty:
                # Clean and prepare movies data
                movies_df = self.movies_df.copy()
                
                # Ensure numeric columns are properly formatted
                numeric_cols = {
                    'runtime': 'Int64',  # Use nullable integer type
                    'vote_average': 'float64',
                    'vote_count': 'Int64',
                    'popularity': 'float64',
                    'budget': 'Int64',
                    'revenue': 'Int64'
                }
                
                for col, dtype in numeric_cols.items():
                    if col in movies_df.columns:
                        if dtype == 'Int64':
                            # Handle integer columns with proper NA handling
                            # First convert to float to handle any decimal points
                            temp_col = pd.to_numeric(movies_df[col], errors='coerce')
                            # Round to nearest integer and convert to Int64
                            movies_df[col] = temp_col.round().astype('Int64')
                        else:
                            # Handle float columns with consistent decimal places
                            movies_df[col] = pd.to_numeric(movies_df[col], errors='coerce').round(4)
                
                # Ensure date column is properly formatted
                if 'release_date' in movies_df.columns:
                    # Convert to datetime with explicit format
                    movies_df['release_date'] = pd.to_datetime(
                        movies_df['release_date'],
                        format='%Y-%m-%d',  # Expected format: YYYY-MM-DD
                        errors='coerce'  # Convert invalid dates to NaT
                    )
                    # Fill NaT with a default date (e.g., January 1st of the year)
                    movies_df['release_date'] = movies_df['release_date'].fillna(
                        pd.to_datetime('1970-01-01')
                    )
                
                # Clean string columns
                string_cols = ['title', 'original_title', 'overview', 'status', 'poster_path', 'backdrop_path']
                for col in string_cols:
                    if col in movies_df.columns:
                        # Replace NaN with empty string
                        movies_df[col] = movies_df[col].fillna('')
                        # Remove any newlines and extra whitespace
                        movies_df[col] = movies_df[col].str.replace('\n', ' ').str.replace('\r', ' ').str.strip()
                        # Handle any remaining unescaped commas in text fields
                        if col in ['overview', 'title', 'original_title']:
                            movies_df[col] = movies_df[col].str.replace(',', ' ')
                
                # Ensure all string columns are properly encoded
                for col in string_cols:
                    if col in movies_df.columns:
                        movies_df[col] = movies_df[col].apply(
                            lambda x: x.encode('utf-8', errors='ignore').decode('utf-8')
                            if isinstance(x, str) else x
                        )
                
                movies_df.to_csv(self.csv_dir / 'movies.csv', **csv_params)
                logger.info(f"Saved {len(movies_df)} movies")

            # Save credits
            if not self.credits_df.empty:
                # Clean and prepare credits data
                credits_df = self.credits_df.copy()
                # Ensure numeric columns are properly formatted
                if 'credit_order' in credits_df.columns:
                    credits_df['credit_order'] = pd.to_numeric(credits_df['credit_order'], errors='coerce').astype('Int64')
                credits_df.to_csv(self.csv_dir / 'credits.csv', **csv_params)
                logger.info(f"Saved {len(credits_df)} credits")

            # Save people
            if not self.people_df.empty:
                # Clean and prepare people data
                people_df = self.people_df.copy()
                # Ensure numeric columns are properly formatted
                if 'gender' in people_df.columns:
                    people_df['gender'] = pd.to_numeric(people_df['gender'], errors='coerce').astype('Int64')
                people_df.to_csv(self.csv_dir / 'people.csv', **csv_params)
                logger.info(f"Saved {len(people_df)} people")

            # Save genres
            if not self.genres_df.empty:
                # Clean and prepare genres data
                genres_df = self.genres_df.copy()
                genres_df.to_csv(self.csv_dir / 'genres.csv', **csv_params)
                logger.info(f"Saved {len(genres_df)} genres")

        except Exception as e:
            logger.error(f"Error saving CSV files: {str(e)}")
            raise

    def _print_error_summary(self):
        """Print a summary of all errors encountered."""
        logger.info("\nError Summary:")
        logger.info("=" * 50)
        logger.info(f"Movies not found (404): {len(self.error_stats['not_found'])}")
        logger.info(f"Timeout errors: {len(self.error_stats['timeout'])}")
        logger.info(f"API errors: {len(self.error_stats['api_error'])}")
        logger.info(f"Processing errors: {len(self.error_stats['processing_error'])}")
        logger.info(f"Validation errors: {len(self.error_stats['validation_error'])}")
        logger.info(f"Removed movies: {len(self.error_stats['removed_movies'])}")
        logger.info(f"Rate limit hits: {len(self.error_stats['rate_limit'])}")
        logger.info(f"Movies without credits: {len(self.error_stats['no_credits'])}")
        logger.info(f"Movies without actors: {len(self.error_stats['no_actors'])}")
        
        if self.error_stats['not_found']:
            logger.info("\nAll not found movie IDs:")
            logger.info(list(self.error_stats['not_found']))
        
        if self.error_stats['timeout']:
            logger.info("\nSample of timeout movie IDs:")
            logger.info(list(self.error_stats['timeout'])[:10])
        
        if self.error_stats['api_error']:
            logger.info("\nSample of API error movie IDs:")
            logger.info(list(self.error_stats['api_error'])[:10])
        
        if self.error_stats['processing_error']:
            logger.info("\nSample of processing error movie IDs:")
            logger.info(list(self.error_stats['processing_error'])[:10])
        
        if self.error_stats['validation_error']:
            logger.info("\nSample of validation error movie IDs:")
            logger.info(list(self.error_stats['validation_error'])[:10])
        
        if self.error_stats['removed_movies']:
            logger.info("\nSample of removed movie IDs:")
            logger.info(list(self.error_stats['removed_movies'])[:10])
        
        if self.error_stats['rate_limit']:
            logger.info("\nSample of rate limit hit movie IDs:")
            logger.info(list(self.error_stats['rate_limit'])[:10])
        
        if self.error_stats['no_credits']:
            logger.info("\nSample of movies without credits:")
            logger.info(list(self.error_stats['no_credits'])[:10])
        
        if self.error_stats['no_actors']:
            logger.info("\nSample of movies without actors:")
            logger.info(list(self.error_stats['no_actors'])[:10])
        
        logger.info("=" * 50)

    def run(self, batch_size: int = 100, max_workers: int = 10, test_year: int = None):
        """Run the ETL process."""
        try:
            logger.info("Starting TMDB ETL process...")
            
            # Get all movie IDs
            movie_ids = self.client.get_movie_ids(test_year=test_year)
            total_movies = len(movie_ids)
            logger.info(f"Found {total_movies} movies to process")

            # Process movies in batches with better memory management
            for i in tqdm(range(0, total_movies, batch_size), desc="Processing batches"):
                if self.interrupted:
                    break

                batch = movie_ids[i:i + batch_size]
                retry_count = 0
                max_retries = 5  # Increased from 3 to 5
                
                while retry_count < max_retries:
                    try:
                        # Process batch with parallel processing
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            # Submit tasks in smaller chunks to avoid overwhelming the connection pool
                            chunk_size = 15  # Reduced chunk size for better memory management
                            for j in range(0, len(batch), chunk_size):
                                chunk = batch[j:j + chunk_size]
                                # Submit all tasks for this chunk
                                future_to_id = {
                                    executor.submit(self._process_movie, movie_id): movie_id 
                                    for movie_id in chunk
                                }
                                
                                # Process results as they complete
                                for future in as_completed(future_to_id):
                                    try:
                                        data = future.result()
                                        if data:
                                            self._append_to_dataframes(data)
                                    except Exception as e:
                                        logger.error(f"Error processing movie {future_to_id[future]}: {str(e)}")
                                        time.sleep(0.1)  # Reduced delay on error
                                
                                # Minimal delay between chunks
                                time.sleep(0.1)
                        
                        # If we get here, the batch was successful
                        break
                        
                    except Exception as e:
                        retry_count += 1
                        logger.error(f"Batch failed (attempt {retry_count}/{max_retries}): {str(e)}")
                        if retry_count < max_retries:
                            wait_time = 2 ** retry_count  # Exponential backoff
                            logger.info(f"Waiting {wait_time} seconds before retry...")
                            time.sleep(wait_time)
                        else:
                            logger.error("Max retries reached, skipping batch")

                # Save progress more frequently and clear memory
                if len(self.movies_df) % 250 == 0:  # More frequent saves
                    self._save_dataframes()
                    # Clear memory more aggressively
                    gc.collect()
                    # Clear person cache more frequently
                    self.person_cache.clear()
                    # Clear DataFrames after saving
                    self.movies_df = pd.DataFrame()
                    self.credits_df = pd.DataFrame()
                    self.people_df = pd.DataFrame()
                    self.genres_df = pd.DataFrame()

            # Final save
            self._save_dataframes()
            
            # Print error summary
            self._print_error_summary()
            
            logger.info("ETL process completed successfully")

        except Exception as e:
            logger.error(f"Error in ETL process: {str(e)}")
            # Save any data we have
            self._save_dataframes()
            # Print error summary even if process failed
            self._print_error_summary()
            raise

def clear_log_files():
    """Clear all log files before starting a new run."""
    log_files = [
        'logs/database.log',
        'logs/schema.log',
        'logs/schema_check.log',
        'logs/tmdb_api.log',
        'logs/tmdb_etl.log',
        'logs/tmdb_load.log'
    ]
    
    for log_file in log_files:
        try:
            with open(log_file, 'w') as f:
                f.write('')  # Clear the file
            logger.info(f"Cleared log file: {log_file}")
        except Exception as e:
            logger.warning(f"Could not clear log file {log_file}: {str(e)}")

def main():
    """Main entry point."""
    import argparse
    parser = argparse.ArgumentParser(description='TMDB ETL Process - Initial Data Load')
    parser.add_argument('--initial', action='store_true', help='Flag to indicate this is initial data loading')
    parser.add_argument('--batch-size', type=int, default=100, help='Number of movies to process in each batch')
    parser.add_argument('--max-workers', type=int, default=10, help='Maximum number of parallel workers')
    parser.add_argument('--test-year', type=int, help='Test with a single year (e.g., 2024)')
    args = parser.parse_args()

    if not args.initial:
        logger.error("This script is for initial data loading only. Use --initial flag to proceed.")
        sys.exit(1)

    # Clear log files before starting
    clear_log_files()

    logger.info("Starting initial TMDB data load...")
    etl = TMDBETL()
    etl.run(batch_size=args.batch_size, max_workers=args.max_workers, test_year=args.test_year)

if __name__ == '__main__':
    main()