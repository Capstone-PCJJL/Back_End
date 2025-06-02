import os
import yaml
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path
import logging
from tqdm import tqdm
import argparse
import time
import concurrent.futures
from functools import partial
import json
import math
from sqlalchemy import text
import pandas as pd

from src.api.tmdb_client import TMDBClient
from src.data.movielens_loader import MovieLensLoader
from src.database.db_manager import DatabaseManager
from src.utils.yaml_handler import YAMLHandler

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MovieETL:
    """ETL pipeline for movie data."""
    
    def __init__(self, batch_size: int = 200, max_workers: int = 20):
        """
        Initialize the ETL pipeline.
        
        Args:
            batch_size: Number of movies to process in each batch
            max_workers: Maximum number of parallel workers
        """
        self.movie_data_dir = "data/movies"
        self.changes_dir = "data/changes"
        self.cache_dir = "data/cache"
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.tmdb_client = TMDBClient()
        self.movielens_loader = MovieLensLoader()
        self.db_manager = DatabaseManager()
        
        # Create necessary directories
        os.makedirs(self.movie_data_dir, exist_ok=True)
        os.makedirs(self.changes_dir, exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_cache_path(self, tmdb_id: int, data_type: str) -> Path:
        """Get cache file path for a movie's data."""
        return Path(self.cache_dir) / f"{tmdb_id}_{data_type}.json"

    def _load_from_cache(self, tmdb_id: int, data_type: str) -> Optional[Dict]:
        """Load movie data from cache if available."""
        cache_path = self._get_cache_path(tmdb_id, data_type)
        if cache_path.exists():
            try:
                with open(cache_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Error loading cache for {tmdb_id} {data_type}: {str(e)}")
        return None

    def _save_to_cache(self, tmdb_id: int, data_type: str, data: Dict) -> None:
        """Save movie data to cache."""
        cache_path = self._get_cache_path(tmdb_id, data_type)
        try:
            with open(cache_path, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.warning(f"Error saving cache for {tmdb_id} {data_type}: {str(e)}")

    def _enrich_movie_batch(self, movie_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich a batch of movies with TMDB data in parallel.
        
        Args:
            movie_batch: List of movie data dictionaries
            
        Returns:
            List[Dict[str, Any]]: List of enriched movie data
        """
        enriched_movies = []
        
        def process_movie(movie_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            try:
                tmdb_id = movie_data.get('tmdbId')
                if not tmdb_id:
                    return movie_data

                # Try to load from cache first
                details = self._load_from_cache(tmdb_id, 'details')
                if not details:
                    details = self.tmdb_client.get_movie_details(tmdb_id)
                    self._save_to_cache(tmdb_id, 'details', details)
                    time.sleep(0.25)  # Rate limiting

                credits = self._load_from_cache(tmdb_id, 'credits')
                if not credits:
                    credits = self.tmdb_client.get_movie_credits(tmdb_id)
                    self._save_to_cache(tmdb_id, 'credits', credits)
                    time.sleep(0.25)

                keywords = self._load_from_cache(tmdb_id, 'keywords')
                if not keywords:
                    keywords = self.tmdb_client.get_movie_keywords(tmdb_id)
                    self._save_to_cache(tmdb_id, 'keywords', keywords)
                    time.sleep(0.25)

                # Clean tags data - remove None/NaN values and convert to list of strings
                tags = movie_data.get('tags', [])
                if isinstance(tags, list):
                    # Clean and sanitize tags
                    cleaned_tags = []
                    for tag in tags:
                        if tag is not None and not (isinstance(tag, float) and math.isnan(tag)):
                            # Convert to string and clean
                            tag_str = str(tag).strip()
                            if tag_str:
                                # Remove any problematic characters
                                tag_str = tag_str.replace('\u0000', '')  # Remove null bytes
                                tag_str = tag_str.replace('\u2028', '')  # Remove line separators
                                tag_str = tag_str.replace('\u2029', '')  # Remove paragraph separators
                                # Escape single quotes and backslashes
                                tag_str = tag_str.replace('\\', '\\\\').replace("'", "\\'")
                                cleaned_tags.append(tag_str)
                    tags = cleaned_tags
                else:
                    tags = []

                # Combine all data
                enriched_data = {
                    'tmdbId': tmdb_id,
                    'movieId': movie_data.get('movieId'),
                    'title': details.get('title'),
                    'original_title': details.get('original_title'),
                    'release_date': details.get('release_date'),
                    'overview': details.get('overview'),
                    'poster_path': details.get('poster_path'),
                    'backdrop_path': details.get('backdrop_path'),
                    'adult': details.get('adult', False),
                    'original_language': details.get('original_language'),
                    'runtime': details.get('runtime'),
                    'status': details.get('status'),
                    'tagline': details.get('tagline'),
                    'popularity': details.get('popularity'),
                    'vote_average': details.get('vote_average'),
                    'vote_count': details.get('vote_count'),
                    'average_rating': movie_data.get('average_rating'),
                    'num_ratings': movie_data.get('num_ratings'),
                    'tags': tags,  # Use cleaned tags
                    'genres': details.get('genres', []),
                    'credits': credits,
                    'keywords': keywords
                }
                
                return enriched_data
                
            except Exception as e:
                logger.error(f"Error enriching movie data: {str(e)}")
                logger.error(f"Movie data that caused error: {movie_data}")
                return None

        # Process movies in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(process_movie, movie) for movie in movie_batch]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    enriched_movies.append(result)

        return enriched_movies

    def _process_credits(self, movie_id: int, credits_data: Dict[str, Any]) -> None:
        """
        Process movie credits data.
        
        Args:
            movie_id: Movie ID in our database
            credits_data: Credits data from TMDB API
        """
        session = self.db_manager.Session()
        try:
            # Process cast
            for cast_member in credits_data.get('cast', []):
                person = session.query(self.db_manager.Person).filter_by(tmdb_id=cast_member['id']).first()
                if not person:
                    person = self.db_manager.Person(
                        tmdb_id=cast_member['id'],
                        name=cast_member['name'],
                        profile_path=cast_member.get('profile_path'),
                        gender=cast_member.get('gender'),
                        popularity=cast_member.get('popularity')
                    )
                    session.add(person)
                    session.flush()
                
                credit = self.db_manager.MovieCredit(
                    movie_id=movie_id,
                    person_id=person.id,
                    credit_type='cast',
                    character=cast_member.get('character'),
                    order=cast_member.get('order')
                )
                session.add(credit)
            
            # Process crew
            for crew_member in credits_data.get('crew', []):
                person = session.query(self.db_manager.Person).filter_by(tmdb_id=crew_member['id']).first()
                if not person:
                    person = self.db_manager.Person(
                        tmdb_id=crew_member['id'],
                        name=crew_member['name'],
                        profile_path=crew_member.get('profile_path'),
                        gender=crew_member.get('gender'),
                        popularity=crew_member.get('popularity')
                    )
                    session.add(person)
                    session.flush()
                
                credit = self.db_manager.MovieCredit(
                    movie_id=movie_id,
                    person_id=person.id,
                    credit_type='crew',
                    department=crew_member.get('department'),
                    job=crew_member.get('job')
                )
                session.add(credit)
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing credits for movie {movie_id}: {str(e)}")
            raise
        finally:
            session.close()

    def _process_videos(self, movie_id: int, videos_data: Dict[str, Any]) -> None:
        """
        Process movie videos data.
        
        Args:
            movie_id: Movie ID in our database
            videos_data: Videos data from TMDB API
        """
        session = self.db_manager.Session()
        try:
            for video in videos_data.get('results', []):
                video_record = self.db_manager.MovieVideo(
                    movie_id=movie_id,
                    tmdb_id=video['id'],
                    name=video['name'],
                    key=video['key'],
                    site=video['site'],
                    type=video['type'],
                    size=video.get('size'),
                    official=video.get('official', False),
                    published_at=datetime.strptime(video['published_at'], '%Y-%m-%dT%H:%M:%S.%fZ') if video.get('published_at') else None
                )
                session.add(video_record)
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing videos for movie {movie_id}: {str(e)}")
            raise
        finally:
            session.close()

    def _process_reviews(self, movie_id: int, reviews_data: Dict[str, Any]) -> None:
        """
        Process movie reviews data.
        
        Args:
            movie_id: Movie ID in our database
            reviews_data: Reviews data from TMDB API
        """
        session = self.db_manager.Session()
        try:
            for review in reviews_data.get('results', []):
                review_record = self.db_manager.MovieReview(
                    movie_id=movie_id,
                    tmdb_id=review['id'],
                    author=review['author'],
                    content=review['content'],
                    url=review['url'],
                    created_at=datetime.strptime(review['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ') if review.get('created_at') else None,
                    updated_at=datetime.strptime(review['updated_at'], '%Y-%m-%dT%H:%M:%S.%fZ') if review.get('updated_at') else None
                )
                session.add(review_record)
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing reviews for movie {movie_id}: {str(e)}")
            raise
        finally:
            session.close()

    def _process_keywords(self, movie_id: int, keywords_data: Dict[str, Any]) -> None:
        """
        Process movie keywords data.
        
        Args:
            movie_id: Movie ID in our database
            keywords_data: Keywords data from TMDB API
        """
        session = self.db_manager.Session()
        try:
            for keyword in keywords_data.get('keywords', []):
                # Check if keyword already exists
                existing_keyword = session.query(self.db_manager.MovieKeyword).filter_by(
                    tmdb_id=keyword['id']
                ).first()
                
                if existing_keyword:
                    # Skip if keyword already exists
                    continue
                
                keyword_record = self.db_manager.MovieKeyword(
                    movie_id=movie_id,
                    tmdb_id=keyword['id'],
                    name=keyword['name']
                )
                session.add(keyword_record)
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing keywords for movie {movie_id}: {str(e)}")
            raise
        finally:
            session.close()

    def _process_release_dates(self, movie_id: int, release_dates_data: Dict[str, Any]) -> None:
        """
        Process movie release dates data.
        
        Args:
            movie_id: Movie ID in our database
            release_dates_data: Release dates data from TMDB API
        """
        session = self.db_manager.Session()
        try:
            for country_data in release_dates_data.get('results', []):
                country = country_data['iso_3166_1']
                for release in country_data.get('release_dates', []):
                    # Handle different date formats
                    release_date = None
                    if release.get('release_date'):
                        try:
                            # Try parsing with timezone
                            release_date = datetime.strptime(release['release_date'].split('T')[0], '%Y-%m-%d')
                        except ValueError:
                            logger.debug(f"Could not parse release date: {release['release_date']}")
                            continue
                    
                    release_record = self.db_manager.MovieReleaseDate(
                        movie_id=movie_id,
                        country=country,
                        certification=release.get('certification'),
                        release_date=release_date,
                        type=release.get('type')
                    )
                    session.add(release_record)
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing release dates for movie {movie_id}: {str(e)}")
            raise
        finally:
            session.close()

    def _process_content_ratings(self, movie_id: int, content_ratings_data: Dict[str, Any]) -> None:
        """
        Process movie content ratings data.
        
        Args:
            movie_id: Movie ID in our database
            content_ratings_data: Content ratings data from TMDB API
        """
        session = self.db_manager.Session()
        try:
            for country_data in content_ratings_data.get('results', []):
                country = country_data['iso_3166_1']
                for rating in country_data.get('rating', []):
                    rating_record = self.db_manager.MovieContentRating(
                        movie_id=movie_id,
                        country=country,
                        rating=rating.get('rating'),
                        meaning=rating.get('meaning')
                    )
                    session.add(rating_record)
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing content ratings for movie {movie_id}: {str(e)}")
            raise
        finally:
            session.close()

    def _process_watch_providers(self, movie_id: int, watch_providers_data: Dict[str, Any]) -> None:
        """
        Process movie watch providers data.
        
        Args:
            movie_id: Movie ID in our database
            watch_providers_data: Watch providers data from TMDB API
        """
        session = self.db_manager.Session()
        try:
            # The watch providers data structure is:
            # {
            #   'results': {
            #     'US': {
            #       'flatrate': [{'provider_id': 8, 'provider_name': 'Netflix', ...}],
            #       'rent': [{'provider_id': 2, 'provider_name': 'Apple TV', ...}],
            #       'buy': [{'provider_id': 2, 'provider_name': 'Apple TV', ...}]
            #     }
            #   }
            # }
            
            results = watch_providers_data.get('results', {})
            if not results:
                return
                
            for country, providers in results.items():
                if not isinstance(providers, dict):
                    continue
                    
                for provider_type, provider_list in providers.items():
                    if not isinstance(provider_list, list):
                        continue
                        
                    for provider in provider_list:
                        if not isinstance(provider, dict):
                            continue
                            
                        provider_record = self.db_manager.MovieWatchProvider(
                            movie_id=movie_id,
                            provider_id=provider.get('provider_id'),
                            provider_name=provider.get('provider_name'),
                            provider_type=provider_type,
                            country=country,
                            logo_path=provider.get('logo_path'),
                            display_priority=provider.get('display_priority')
                        )
                        session.add(provider_record)
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing watch providers for movie {movie_id}: {str(e)}")
            raise
        finally:
            session.close()

    def get_latest_release_date(self) -> Optional[datetime]:
        """
        Get the most recent release date in the database.
        Returns:
            datetime or None
        """
        session = self.db_manager.Session()
        try:
            latest_movie = session.query(self.db_manager.Movie)\
                .filter(self.db_manager.Movie.release_date.isnot(None))\
                .order_by(self.db_manager.Movie.release_date.desc())\
                .first()
            if latest_movie and latest_movie.release_date:
                return latest_movie.release_date
            return None
        finally:
            session.close()

    def initial_load(self, start_year: int = None, end_year: int = None) -> None:
        """
        Perform initial data load from MovieLens and TMDB.
        
        Args:
            start_year: Optional start year for TMDB data
            end_year: Optional end year for TMDB data
        """
        try:
            # Load MovieLens data
            logger.info("Loading MovieLens data...")
            self.movielens_loader.load_data()
            
            # Get TMDB data
            logger.info("Getting recent movies from TMDB...")
            latest_date = self.db_manager.get_latest_release_date()
            if not latest_date:
                latest_date = datetime(2023, 1, 1)
                logger.info(f"No release date found in database, using default date: {latest_date.date()}")
            
            logger.info(f"Getting movies from {latest_date.date()} to present")
            
            # Process by year
            current_year = datetime.now().year
            for year in range(start_year or latest_date.year, end_year or current_year + 1):
                logger.info(f"Fetching movies for year {year}")
                movies = self.tmdb_client.get_movies_by_year(year)
                
                # Save to YAML
                yaml_handler = YAMLHandler()
                filepath = yaml_handler.save_movie_batch(movies, 'initial', str(year))
                logger.info(f"Saved {len(movies)} movies for {year} to {filepath}")
            
            logger.info("Use yaml_processor.py to load the movies into the database")
            
        except Exception as e:
            logger.error(f"Error in initial load: {str(e)}")
            raise

    def process_missing_movies(self, after_date: str = None) -> None:
        """
        Process movies that are in TMDB but not in the database.
        
        Args:
            after_date: Optional date to filter movies after
        """
        try:
            # Get movies from TMDB
            movies = self.tmdb_client.get_movies_after_date(after_date)
            logger.info(f"Found {len(movies)} movies in TMDB after {after_date}")
            
            # Save to YAML
            yaml_handler = YAMLHandler()
            filepath = yaml_handler.save_movie_batch(movies, 'missing', datetime.now().strftime("%Y%m%d"))
            logger.info(f"Saved movies to {filepath}")
            
            logger.info("Use yaml_processor.py to load the movies into the database")
            
        except Exception as e:
            logger.error(f"Error processing missing movies: {str(e)}")
            raise

    def process_changes(self, days: int = 1) -> None:
        """
        Process recent movie changes.
        
        Args:
            days: Number of days to look back
        """
        try:
            # Get changes from TMDB
            changes = self.tmdb_client.get_recent_changes(days)
            logger.info(f"Found {len(changes)} changes in the last {days} days")
            
            # Save to YAML
            yaml_handler = YAMLHandler()
            filepath = yaml_handler.save_movie_batch(changes, 'changes', datetime.now().strftime("%Y%m%d"))
            logger.info(f"Saved changes to {filepath}")
            
            logger.info("Use yaml_processor.py to load the changes into the database")
            
        except Exception as e:
            logger.error(f"Error processing changes: {str(e)}")
            raise

    def update_movie(self, tmdb_id: int) -> None:
        """
        Update a specific movie's data.
        
        Args:
            tmdb_id: TMDB movie ID
        """
        try:
            # Get the latest movie data
            movie_data = self._enrich_movie_batch([{'tmdbId': tmdb_id}])[0]
            
            if not movie_data:  # Skip if None (adult content)
                return

            # Check if movie already exists
            session = self.db_manager.Session()
            try:
                existing_movie = session.query(self.db_manager.Movie).filter_by(tmdb_id=tmdb_id).first()
                if existing_movie:
                    logging.info(f"Movie '{movie_data.get('title', 'Unknown')}' already exists in database.")
                    return
            finally:
                session.close()

            # Show movie details and ask for confirmation
            print("\nMovie Details:")
            print(f"Title: {movie_data.get('title', 'Unknown')}")
            print(f"Original Title: {movie_data.get('original_title', 'Unknown')}")
            print(f"Release Date: {movie_data.get('release_date', 'Unknown')}")
            print(f"Overview: {movie_data.get('overview', 'No overview available')[:200]}...")
            print(f"Genres: {', '.join(genre['name'] for genre in movie_data.get('genres', []))}")
            
            # Ask for confirmation
            while True:
                confirm = input("\nDo you want to add this movie to the database? (y/n): ").lower()
                if confirm in ['y', 'n']:
                    break
                print("Please enter 'y' for yes or 'n' for no.")
            
            if confirm != 'y':
                logging.info("Movie addition cancelled by user.")
                return
            
            # Update in database
            self.db_manager.load_movie_data(movie_data)
            
            # Get the movie ID from our database
            db_movie = self.db_manager.get_movie_by_tmdb_id(tmdb_id)
            if db_movie:
                # Process additional data
                self._process_credits(db_movie.id, movie_data.get('credits', {}))
                self._process_videos(db_movie.id, movie_data.get('videos', {}))
                self._process_reviews(db_movie.id, movie_data.get('reviews', {}))
                self._process_keywords(db_movie.id, movie_data.get('keywords', {}))
                self._process_release_dates(db_movie.id, movie_data.get('release_dates', {}))
                self._process_content_ratings(db_movie.id, movie_data.get('content_ratings', {}))
                self._process_watch_providers(db_movie.id, movie_data.get('watch_providers', {}))
            
            logging.info(f"✅ Added movie {movie_data.get('title', 'Unknown')}")
            
        except Exception as e:
            logging.error(f"Error updating movie {tmdb_id}: {str(e)}")
            raise

    def clear_database(self) -> None:
        """
        Clear all data from the database tables.
        This will remove all records while keeping the table structure intact.
        """
        try:
            self.db_manager.clear_database()
        except Exception as e:
            logger.error(f"Error clearing database: {str(e)}")
            raise

def main():
    """Main function to run the ETL pipeline."""
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Movie ETL Pipeline')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Initial load command
    init_parser = subparsers.add_parser('init', help='Perform initial data load')
    init_parser.add_argument('--start-year', type=int, default=1900,
                           help='Year to start loading from (default: 1900)')
    init_parser.add_argument('--end-year', type=int, default=datetime.now().year,
                           help='Year to end loading at (default: current year)')
    init_parser.add_argument('--full', action='store_true',
                           help='Run all steps in sequence (MovieLens load, TMDB fetch, and YAML processing)')
    init_parser.add_argument('--load-movielens', action='store_true',
                           help='Load MovieLens data into the database')
    init_parser.add_argument('--fetch-tmdb', action='store_true',
                           help='Fetch TMDB data and save to YAML files')
    
    # Update changes command
    changes_parser = subparsers.add_parser('changes', help='Process recent changes')
    changes_parser.add_argument('--days', type=int, default=1,
                              help='Number of days to look back for changes (default: 1)')
    
    # Update specific movie command
    update_parser = subparsers.add_parser('update', help='Update a specific movie')
    update_parser.add_argument('tmdb_id', type=int,
                             help='TMDB ID of the movie to update')
    
    # Clear database command
    clear_parser = subparsers.add_parser('clear', help='Clear all data from the database')
    clear_parser.add_argument('--force', action='store_true',
                            help='Force clear without confirmation')
    
    # Missing movies command
    missing_parser = subparsers.add_parser('missing', help='Get missing movies from TMDB')
    missing_parser.add_argument('--after-date', type=str, help='Fetch movies released after this date (YYYY-MM-DD)')
    missing_parser.add_argument('--end-year', type=int, default=datetime.now().year, help='Year to end loading at (default: current year)')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Create necessary directories
    os.makedirs("data/movies", exist_ok=True)
    os.makedirs("data/changes", exist_ok=True)
    
    # Initialize ETL pipeline
    etl = MovieETL()
    
    # Create database tables
    etl.db_manager.create_tables()
    
    # Execute command
    if args.command == 'init':
        if args.full:
            logger.info("Running full initial load process")
            # Step 1: Load MovieLens data
            logger.info("Step 1: Loading MovieLens data...")
            etl.movielens_loader.load_data()
            
            # Step 2: Fetch TMDB data
            logger.info("Step 2: Fetching TMDB data...")
            etl.initial_load(start_year=args.start_year, end_year=args.end_year)
            
            # Step 3: Process YAML files
            logger.info("Step 3: Processing YAML files...")
            from src.etl.yaml_processor import YAMLProcessor
            processor = YAMLProcessor()
            processor.process_all_files('initial')
        else:
            if args.load_movielens:
                logger.info("Loading MovieLens data...")
                etl.movielens_loader.load_data()
            if args.fetch_tmdb:
                logger.info(f"Fetching TMDB data from {args.start_year} to {args.end_year}")
                etl.initial_load(start_year=args.start_year, end_year=args.end_year)
            if not (args.load_movielens or args.fetch_tmdb):
                logger.info("No action specified. Use --load-movielens, --fetch-tmdb, or --full")
    elif args.command == 'changes':
        logger.info(f"Processing changes from the last {args.days} days")
        etl.process_changes(days=args.days)
    elif args.command == 'update':
        logger.info(f"Updating movie with TMDB ID: {args.tmdb_id}")
        etl.update_movie(args.tmdb_id)
    elif args.command == 'clear':
        if not args.force:
            confirm = input("⚠️  This will delete ALL data from the database. Are you sure? (y/N): ")
            if confirm.lower() != 'y':
                logger.info("Operation cancelled")
                return
        etl.clear_database()
    elif args.command == 'missing':
        after_date = None
        if args.after_date:
            after_date = datetime.strptime(args.after_date, '%Y-%m-%d')
        else:
            # Use latest release date in DB
            after_date = etl.get_latest_release_date()
            if after_date:
                logger.info(f"Using after_date {after_date.date()} (after latest movie in DB)")
        logger.info(f"Getting missing movies after {after_date.date() if after_date else 'N/A'} to {args.end_year}")
        etl.process_missing_movies(after_date=after_date, end_year=args.end_year)
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 