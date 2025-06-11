import logging
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
from tqdm import tqdm
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc

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
        
        # Set up signal handler
        signal.signal(signal.SIGINT, self._signal_handler)
        self.interrupted = False
        self.person_cache = {}  # Cache for person data

    def _signal_handler(self, signum, frame):
        """Handle interrupt signal."""
        logger.info("Process interrupted by user. Exiting gracefully.")
        self.interrupted = True
        sys.exit(0)

    def _process_movie(self, movie_id: int) -> Optional[Dict]:
        """Process a single movie and its related data."""
        try:
            # Get movie details with caching
            movie_data = self.client.get_movie_details(movie_id)
            if not movie_data:
                return None

            # Get credits with caching
            credits_data = self.client.get_movie_credits(movie_id)
            if not credits_data:
                return None

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
                    except Exception as e:
                        logger.error(f"Error getting person details for ID {person_futures[future]}: {str(e)}")

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

        except Exception as e:
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
        """Save all DataFrames to CSV files."""
        logger.info("Saving data to CSV files...")
        
        # Save movies
        if not self.movies_df.empty:
            self.movies_df.to_csv(self.csv_dir / 'movies.csv', index=False)
            logger.info(f"Saved {len(self.movies_df)} movies")

        # Save credits
        if not self.credits_df.empty:
            self.credits_df.to_csv(self.csv_dir / 'credits.csv', index=False)
            logger.info(f"Saved {len(self.credits_df)} credits")

        # Save people
        if not self.people_df.empty:
            self.people_df.to_csv(self.csv_dir / 'people.csv', index=False)
            logger.info(f"Saved {len(self.people_df)} people")

        # Save genres
        if not self.genres_df.empty:
            self.genres_df.to_csv(self.csv_dir / 'genres.csv', index=False)
            logger.info(f"Saved {len(self.genres_df)} genres")

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
                max_retries = 3
                
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
                            time.sleep(1)  # Reduced wait before retrying
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
            logger.info("ETL process completed successfully")

        except Exception as e:
            logger.error(f"Error in ETL process: {str(e)}")
            # Save any data we have
            self._save_dataframes()
            raise

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

    logger.info("Starting initial TMDB data load...")
    etl = TMDBETL()
    etl.run(batch_size=args.batch_size, max_workers=args.max_workers, test_year=args.test_year)

if __name__ == '__main__':
    main()