import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import argparse
from tqdm import tqdm
import yaml
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from fuzzywuzzy import fuzz

from src.api.tmdb_client import TMDBClient
from src.database.db_manager import DatabaseManager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TMDBETL:
    """ETL pipeline for TMDB movie data."""
    
    def __init__(self, batch_size=20, max_workers=5):
        """Initialize the ETL pipeline."""
        self.tmdb_client = TMDBClient()
        self.db_manager = DatabaseManager()
        self.yaml_dir = Path("data/yaml")
        self.yaml_dir.mkdir(parents=True, exist_ok=True)

    def initial_load(self, start_year: int = None, end_year: int = None) -> None:
        """
        Perform initial TMDB data load.
        This will load all movies from the specified year range.
        
        Args:
            start_year: Optional start year for TMDB data
            end_year: Optional end year for TMDB data (defaults to current year)
        """
        session = None
        try:
            session = self.db_manager.Session()
            
            # Clear existing TMDB data
            logger.info("Clearing existing TMDB data...")
            session.query(self.db_manager.TMDBMovieGenre).delete()
            session.query(self.db_manager.TMDBMovie).delete()
            session.commit()
            logger.info("TMDB tables cleared successfully")
            
            # Get movies by year
            if not start_year:
                # Get the latest year from the movies table
                latest_movie = session.query(self.db_manager.Movie)\
                    .filter(self.db_manager.Movie.release_date.isnot(None))\
                    .order_by(self.db_manager.Movie.release_date.desc())\
                    .first()
                
                if latest_movie and latest_movie.release_date:
                    start_year = latest_movie.release_date.year
                    logger.info(f"Using latest year from database: {start_year}")
                else:
                    start_year = 2023  # Default to 2023 if no movies found
                    logger.info(f"No movies found in database, using default year: {start_year}")
            
            # Set end year to current year if not specified
            if not end_year:
                end_year = datetime.now().year
                logger.info(f"Using current year as end year: {end_year}")
            
            logger.info(f"Loading movies from {start_year} to {end_year}")
            
            for year in range(start_year, end_year + 1):
                logger.info(f"Fetching movies for year {year}")
                movies = self.tmdb_client.get_movies_by_year(year)
                
                # Filter out adult content
                movies = [m for m in movies if not m.get('adult', False)]
                total_movies = len(movies)
                logger.info(f"Found {total_movies} movies to process for year {year}")
                
                # Load all movies for the year in a single batch
                self._load_movies_batch(movies)
                
                logger.info(f"Completed processing {total_movies} movies for year {year}")
                
            logger.info("Initial load completed successfully")
            
        except Exception as e:
            logger.error(f"Error in initial load: {str(e)}")
            if session:
                session.rollback()
            raise
        finally:
            if session:
                session.close()

    def process_changes(self, days: int = 1) -> None:
        """
        Process recent movie changes.
        This will fetch changes from TMDB and prompt for human approval before adding to the database.
        
        Args:
            days: Number of days to look back
        """
        try:
            # Get changes from TMDB
            changes = self.tmdb_client.get_recent_changes(days)
            total_changes = len(changes)
            logger.info(f"Found {total_changes} changes in the last {days} days")
            
            # Filter out adult content
            changes = [c for c in changes if not c.get('adult', False)]
            filtered_changes = len(changes)
            if filtered_changes < total_changes:
                logger.info(f"Filtered out {total_changes - filtered_changes} adult content movies")
            
            # Process all changes in a single batch
            self._load_movies_batch(changes)
            logger.info(f"Completed processing {filtered_changes} changes")
                
        except Exception as e:
            logger.error(f"Error processing changes: {str(e)}")
            raise

    def process_missing_movies(self, after_date: Optional[datetime] = None) -> None:
        """
        Process movies that are in TMDB but not in our database.
        This will fetch movies after the specified date and prompt for human approval before adding.
        
        Args:
            after_date: Optional date to filter movies after
        """
        try:
            if not after_date:
                # Get the latest release date from our database
                after_date = self._get_latest_release_date()
                if not after_date:
                    after_date = datetime(2023, 1, 1)
                    logger.info(f"No release date found in database, using default date: {after_date.date()}")
            
            logger.info(f"Getting movies after {after_date.date()}")
            movies = self.tmdb_client.get_movies_after_date(after_date)
            total_movies = len(movies)
            logger.info(f"Found {total_movies} movies to process")
            
            # Filter out adult content
            movies = [m for m in movies if not m.get('adult', False)]
            filtered_movies = len(movies)
            if filtered_movies < total_movies:
                logger.info(f"Filtered out {total_movies - filtered_movies} adult content movies")
            
            # Process all movies in a single batch
            self._load_movies_batch(movies)
            logger.info(f"Completed processing {filtered_movies} movies")
                
        except Exception as e:
            logger.error(f"Error processing missing movies: {str(e)}")
            raise

    def search_movie(self, query: str, search_by: str = 'name') -> None:
        """
        Search for a movie by name or ID and process it.
        
        Args:
            query: Movie name or TMDB ID to search for
            search_by: Either 'name' or 'id'
        """
        try:
            if search_by == 'id':
                try:
                    tmdb_id = int(query)
                    movie_data = self.tmdb_client.get_movie_details(tmdb_id)
                    if movie_data:
                        # Save to YAML for review
                        yaml_file = self._save_to_yaml([movie_data], "search", query)
                        if yaml_file:
                            logger.info(f"Saved movie to {yaml_file} for review")
                            self.review_yaml(yaml_file)
                            self.load_from_yaml(yaml_file)
                    else:
                        logger.error(f"No movie found with ID {tmdb_id}")
                except ValueError:
                    logger.error("Invalid TMDB ID. Please provide a numeric ID.")
            else:  # search by name
                search_results = self.tmdb_client.search_movies(query)
                if not search_results:
                    logger.error(f"No movies found matching '{query}'")
                    return
                
                # Sort results by fuzzy match score
                scored_results = []
                for movie in search_results:
                    title = movie.get('title', '')
                    # Calculate match scores for both title and original title
                    title_score = fuzz.ratio(query.lower(), title.lower())
                    original_title = movie.get('original_title', '')
                    original_score = fuzz.ratio(query.lower(), original_title.lower()) if original_title else 0
                    # Use the higher score
                    score = max(title_score, original_score)
                    scored_results.append((movie, score))
                
                # Sort by score in descending order
                scored_results.sort(key=lambda x: x[1], reverse=True)
                
                print(f"\nFound {len(scored_results)} movies matching '{query}':")
                for i, (movie, score) in enumerate(scored_results, 1):
                    print(f"\n{i}. {movie.get('title', 'Unknown')} ({movie.get('release_date', 'Unknown')})")
                    print(f"   ID: {movie.get('id')}")
                    print(f"   Match Score: {score}%")
                    print(f"   Overview: {movie.get('overview', 'No overview available')[:100]}...")
                
                while True:
                    try:
                        choice = input("\nEnter the number of the movie to process (or 'q' to quit): ")
                        if choice.lower() == 'q':
                            return
                        
                        choice = int(choice)
                        if 1 <= choice <= len(scored_results):
                            selected_movie = scored_results[choice - 1][0]
                            # Get full movie details
                            movie_data = self.tmdb_client.get_movie_details(selected_movie['id'])
                            if movie_data:
                                # Save to YAML for review
                                yaml_file = self._save_to_yaml([movie_data], "search", query)
                                if yaml_file:
                                    logger.info(f"Saved movie to {yaml_file} for review")
                                    self.review_yaml(yaml_file)
                                    self.load_from_yaml(yaml_file)
                            break
                        else:
                            print(f"Please enter a number between 1 and {len(scored_results)}")
                    except ValueError:
                        print("Please enter a valid number or 'q' to quit")
                
        except Exception as e:
            logger.error(f"Error searching for movie: {str(e)}")
            raise

    def _process_single_movie(self, movie_data: Dict[str, Any]) -> None:
        """
        Process a single movie, prompting for human approval.
        
        Args:
            movie_data: Movie data from TMDB
        """
        try:
            # Check if movie already exists
            session = self.db_manager.Session()
            existing_movie = session.query(self.db_manager.TMDBMovie).filter_by(tmdb_id=movie_data['id']).first()
            
            if existing_movie:
                logger.info(f"Movie '{movie_data.get('title', 'Unknown')}' already exists in database.")
                session.close()
                return
            
            # Show movie details and ask for confirmation
            print("\nMovie Details:")
            print(f"Title: {movie_data.get('title', 'Unknown')}")
            print(f"Original Title: {movie_data.get('original_title', 'Unknown')}")
            print(f"Release Date: {movie_data.get('release_date', 'Unknown')}")
            print(f"Overview: {movie_data.get('overview', 'No overview available')[:200]}...")
            print(f"Genres: {', '.join(genre['name'] for genre in movie_data.get('genres', []))}")
            
            # Ask for confirmation
            while True:
                confirm = input("\nDo you want to add this movie to the database? (y/n/s): ").lower()
                if confirm in ['y', 'n', 's']:
                    break
                print("Please enter 'y' for yes, 'n' for no, or 's' to skip.")
            
            if confirm == 's':
                logger.info("Skipping movie.")
                session.close()
                return
            elif confirm != 'y':
                logger.info("Movie addition cancelled by user.")
                session.close()
                return
            
            # Add movie to database
            logger.info(f"Adding movie '{movie_data.get('title', 'Unknown')}' to database...")
            movie = self.db_manager.TMDBMovie(
                tmdb_id=movie_data['id'],
                title=movie_data.get('title'),
                original_title=movie_data.get('original_title'),
                release_date=datetime.strptime(movie_data['release_date'], '%Y-%m-%d') if movie_data.get('release_date') else None,
                overview=movie_data.get('overview'),
                poster_path=movie_data.get('poster_path'),
                backdrop_path=movie_data.get('backdrop_path'),
                adult=movie_data.get('adult', False),
                original_language=movie_data.get('original_language'),
                runtime=movie_data.get('runtime'),
                status=movie_data.get('status'),
                tagline=movie_data.get('tagline'),
                popularity=movie_data.get('popularity'),
                vote_average=movie_data.get('vote_average'),
                vote_count=movie_data.get('vote_count'),
                is_approved=True,
                approval_date=datetime.utcnow()
            )
            session.add(movie)
            session.flush()
            
            # Process genres
            genres = movie_data.get('genres', [])
            logger.info(f"Processing {len(genres)} genres for movie...")
            for genre_data in genres:
                genre = session.query(self.db_manager.Genre).filter_by(name=genre_data['name']).first()
                if not genre:
                    genre = self.db_manager.Genre(name=genre_data['name'])
                    session.add(genre)
                    session.flush()
                
                movie_genre = self.db_manager.TMDBMovieGenre(
                    movie_id=movie.id,
                    genre_id=genre.id
                )
                session.add(movie_genre)
            
            session.commit()
            logger.info(f"✅ Successfully added movie '{movie_data.get('title', 'Unknown')}' with {len(genres)} genres")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing movie: {str(e)}")
            raise
        finally:
            session.close()

    def _load_movies_batch(self, movies: List[Dict[str, Any]]) -> None:
        """
        Load a batch of movies into the database without human approval.
        
        Args:
            movies: List of movie data dictionaries
        """
        session = self.db_manager.Session()
        try:
            # Show a single progress bar for the entire batch
            for movie_data in tqdm(movies, desc=f"Loading {len(movies)} movies", unit="movie"):
                # Get TMDB ID from either field name
                tmdb_id = movie_data.get('tmdb_id') or movie_data.get('id')
                if not tmdb_id:
                    logger.error(f"Movie data missing TMDB ID: {movie_data.get('title', 'Unknown')}")
                    continue

                # Create movie record
                movie = self.db_manager.TMDBMovie(
                    tmdb_id=tmdb_id,
                    title=movie_data.get('title'),
                    original_title=movie_data.get('original_title'),
                    release_date=datetime.strptime(movie_data['release_date'], '%Y-%m-%d') if movie_data.get('release_date') else None,
                    overview=movie_data.get('overview'),
                    poster_path=movie_data.get('poster_path'),
                    backdrop_path=movie_data.get('backdrop_path'),
                    adult=movie_data.get('adult', False),
                    original_language=movie_data.get('original_language'),
                    runtime=movie_data.get('runtime'),
                    status=movie_data.get('status'),
                    tagline=movie_data.get('tagline'),
                    popularity=movie_data.get('popularity'),
                    vote_average=movie_data.get('vote_average'),
                    vote_count=movie_data.get('vote_count'),
                    is_approved=True,
                    approval_date=datetime.utcnow()
                )
                session.add(movie)
                session.flush()
                
                # Process genres
                for genre_data in movie_data.get('genres', []):
                    genre = session.query(self.db_manager.Genre).filter_by(name=genre_data['name']).first()
                    if not genre:
                        genre = self.db_manager.Genre(name=genre_data['name'])
                        session.add(genre)
                        session.flush()
                    
                    movie_genre = self.db_manager.TMDBMovieGenre(
                        movie_id=movie.id,
                        genre_id=genre.id
                    )
                    session.add(movie_genre)
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error loading movies batch: {str(e)}")
            raise
        finally:
            session.close()

    def _get_latest_release_date(self) -> Optional[datetime]:
        """
        Get the most recent release date from both movies and tmdb_movies tables.
        This is used for missing and changes commands to ensure we don't miss any movies.
        
        Returns:
            Optional[datetime]: The latest release date found, or None if no movies exist
        """
        session = self.db_manager.Session()
        try:
            logger.info("Querying both tmdb_movies and movies tables for latest release date...")
            
            # Check tmdb_movies table
            latest_tmdb = session.query(self.db_manager.TMDBMovie)\
                .filter(self.db_manager.TMDBMovie.release_date.isnot(None))\
                .order_by(self.db_manager.TMDBMovie.release_date.desc())\
                .first()
            
            # Check movies table
            latest_movies = session.query(self.db_manager.Movie)\
                .filter(self.db_manager.Movie.release_date.isnot(None))\
                .order_by(self.db_manager.Movie.release_date.desc())\
                .first()
            
            # Compare dates from both tables
            if latest_tmdb and latest_movies:
                latest_date = max(latest_tmdb.release_date, latest_movies.release_date)
                logger.info(f"Found latest date from both tables: {latest_date.date()}")
                return latest_date
            elif latest_tmdb:
                logger.info(f"Found latest date from tmdb_movies table: {latest_tmdb.release_date.date()}")
                return latest_tmdb.release_date
            elif latest_movies:
                logger.info(f"Found latest date from movies table: {latest_movies.release_date.date()}")
                return latest_movies.release_date
            
            logger.warning("No release dates found in either table")
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest release date: {str(e)}")
            return None
        finally:
            session.close()

    def _generate_batch_filename(self, batch_type: str, batch_id: str = None) -> str:
        """Generate a filename for the YAML batch file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if batch_id:
            return f"tmdb_{batch_type}_{batch_id}_{timestamp}.yaml"
        return f"tmdb_{batch_type}_{timestamp}.yaml"

    def _save_to_yaml(self, movies: List[Dict], batch_type: str, batch_id: str = None) -> str:
        """Save movies to a YAML file with metadata."""
        filename = self._generate_batch_filename(batch_type, batch_id)
        filepath = self.yaml_dir / filename
        
        data = {
            "metadata": {
                "batch_type": batch_type,
                "batch_id": batch_id,
                "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
                "movie_count": len(movies)
            },
            "movies": movies
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, allow_unicode=True, sort_keys=False)
        
        return str(filepath)

    def _search_movies(self, query: str, search_by: str = 'name') -> List[Dict]:
        """
        Search for movies and return the results.
        
        Args:
            query: Search query (movie name or ID)
            search_by: Either 'name' or 'id'
            
        Returns:
            List of movie dictionaries
        """
        try:
            if search_by == 'id':
                try:
                    tmdb_id = int(query)
                    movie_data = self.tmdb_client.get_movie_details(tmdb_id)
                    return [movie_data] if movie_data else []
                except ValueError:
                    logger.error("Invalid TMDB ID. Please provide a numeric ID.")
                    return []
            else:  # search by name
                search_results = self.tmdb_client.search_movies(query)
                if not search_results:
                    logger.error(f"No movies found matching '{query}'")
                    return []
                
                # Sort results by fuzzy match score
                scored_results = []
                for movie in search_results:
                    title = movie.get('title', '')
                    # Calculate match scores for both title and original title
                    title_score = fuzz.ratio(query.lower(), title.lower())
                    original_title = movie.get('original_title', '')
                    original_score = fuzz.ratio(query.lower(), original_title.lower()) if original_title else 0
                    # Use the higher score
                    score = max(title_score, original_score)
                    scored_results.append((movie, score))
                
                # Sort by score in descending order
                scored_results.sort(key=lambda x: x[1], reverse=True)
                
                # Get full details for each movie
                movies = []
                for movie, score in scored_results:
                    if score >= 60:  # Only include movies with good match scores
                        full_details = self.tmdb_client.get_movie_details(movie['id'])
                        if full_details:
                            full_details['match_score'] = score
                            movies.append(full_details)
                
                return movies
                
        except Exception as e:
            logger.error(f"Error searching for movies: {str(e)}")
            return []

    def fetch_to_yaml(self, batch_type: str, batch_id: str = None, **kwargs) -> str:
        """Fetch movies and save to YAML without loading to database."""
        movies = []
        
        if batch_type == "initial":
            # For initial load, load directly to database without YAML
            if batch_id:
                year = int(batch_id)
                logger.info(f"Using specified year: {year}")
            else:
                # Get the latest year from the movies_raw table using raw SQL
                session = self.db_manager.Session()
                try:
                    logger.info("Querying movies_raw table for latest year...")
                    query = text("SELECT MAX(year) as latest_year FROM movies_raw WHERE year IS NOT NULL")
                    result = session.execute(query).scalar()
                    
                    if result:
                        year = int(result)
                        logger.info(f"Found latest year in movies_raw table: {year}")
                        # Log some sample movies from this year
                        sample_query = text("""
                            SELECT title, year 
                            FROM movies_raw 
                            WHERE year = :year 
                            LIMIT 5
                        """)
                        samples = session.execute(sample_query, {"year": year}).fetchall()
                        if samples:
                            logger.info("Sample movies from this year:")
                            for sample in samples:
                                logger.info(f"  - {sample.title} ({sample.year})")
                    else:
                        year = 2023  # Default to 2023 if no movies found
                        logger.warning("No movies found in movies_raw table, using default year: 2023")
                finally:
                    session.close()
            
            logger.info(f"Starting initial load for year {year}")
            # Use current year as end year
            current_year = datetime.now().year
            logger.info(f"Using current year ({current_year}) as end year")
            self.initial_load(start_year=year, end_year=current_year)
            return None
        elif batch_type == "missing":
            # For missing movies, batch_id is a date
            if batch_id:
                after_date = datetime.strptime(batch_id, '%Y-%m-%d')
                logger.info(f"Using specified date: {after_date.date()}")
            else:
                # Get the latest release date from the database
                after_date = self._get_latest_release_date()
                if not after_date:
                    after_date = datetime(2023, 1, 1)
                    logger.warning(f"No release date found in database, using default date: {after_date.date()}")
                else:
                    logger.info(f"Found latest release date in database: {after_date.date()}")
            movies = self._fetch_missing_movies(after_date=after_date)
        elif batch_type == "changes":
            logger.info("Fetching recent changes...")
            movies = self._fetch_recent_changes(**kwargs)
        
        if movies:  # Only save to YAML if we have movies
            logger.info(f"Found {len(movies)} movies to process")
            return self._save_to_yaml(movies, batch_type, batch_id)
        else:
            logger.info("No movies found to process")
        return None

    def review_yaml(self, yaml_file: str) -> None:
        """Review movies in a YAML file and update their approval status."""
        with open(yaml_file, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        for movie in data['movies']:
            if movie.get('approval_status') is None:
                print(f"\nReviewing: {movie['title']} ({movie['release_date']})")
                print(f"Overview: {movie['overview']}")
                print(f"Adult: {movie['adult']}")
                
                while True:
                    response = input("Approve this movie? (yes/no/skip): ").lower()
                    if response in ['yes', 'no', 'skip']:
                        break
                    print("Please enter 'yes', 'no', or 'skip'")
                
                if response != 'skip':
                    movie['approval_status'] = response
                    movie['review_date'] = datetime.now().isoformat()
        
        # Save the updated YAML
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, allow_unicode=True, sort_keys=False)

    def load_from_yaml(self, yaml_file: str) -> None:
        """Load approved movies from YAML into database."""
        with open(yaml_file, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        approved_movies = [m for m in data['movies'] if m.get('approval_status') == 'yes']
        
        for movie in approved_movies:
            try:
                self._process_movie(movie)
                print(f"Successfully loaded: {movie['title']}")
            except Exception as e:
                print(f"Error loading {movie['title']}: {str(e)}")
        
        # Archive the YAML file if all movies are reviewed
        if all(m.get('approval_status') is not None for m in data['movies']):
            archive_dir = self.yaml_dir / "archived"
            archive_dir.mkdir(exist_ok=True)
            os.rename(yaml_file, archive_dir / Path(yaml_file).name)

    def fetch_review_load(self, batch_type: str, batch_id: str = None, **kwargs) -> None:
        """Combined command to fetch, review, and load movies."""
        yaml_file = self.fetch_to_yaml(batch_type, batch_id, **kwargs)
        self.review_yaml(yaml_file)
        self.load_from_yaml(yaml_file)

def main():
    """Main function to run the TMDB ETL pipeline."""
    parser = argparse.ArgumentParser(description='TMDB Movie ETL Pipeline')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Initial load command
    init_parser = subparsers.add_parser('init', help='Perform initial TMDB data load')
    init_parser.add_argument('--start-year', type=int, default=2023,
                           help='Year to start loading from (default: 2023)')
    init_parser.add_argument('--end-year', type=int, default=datetime.now().year,
                           help='Year to end loading at (default: current year)')
    
    # Changes command
    changes_parser = subparsers.add_parser('changes', help='Process recent changes')
    changes_parser.add_argument('--days', type=int, default=1,
                              help='Number of days to look back for changes (default: 1)')
    
    # Missing movies command
    missing_parser = subparsers.add_parser('missing', help='Get missing movies from TMDB')
    missing_parser.add_argument('--after-date', type=str,
                              help='Fetch movies released after this date (YYYY-MM-DD)')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search for a movie by name or ID')
    search_parser.add_argument('query', type=str, help='Movie name or TMDB ID to search for')
    search_parser.add_argument('--by', type=str, choices=['name', 'id'], default='name',
                             help='Search by name or ID (default: name)')
    
    # Fetch to YAML command
    fetch_parser = subparsers.add_parser('fetch', help='Fetch movies to YAML')
    fetch_parser.add_argument('--type', required=True, choices=['initial', 'missing', 'changes'],
                            help='Type of fetch operation')
    fetch_parser.add_argument('--batch-id', help='Optional batch identifier')
    
    # Review YAML command
    review_parser = subparsers.add_parser('review', help='Review movies in YAML')
    review_parser.add_argument('--file', required=True, help='Path to YAML file')
    
    # Load from YAML command
    load_parser = subparsers.add_parser('load', help='Load approved movies from YAML')
    load_parser.add_argument('--file', required=True, help='Path to YAML file')
    
    # Combined command
    combined_parser = subparsers.add_parser('fetch-review-load', help='Fetch, review, and load movies')
    combined_parser.add_argument('--type', required=True, choices=['initial', 'missing', 'changes'],
                               help='Type of fetch operation')
    combined_parser.add_argument('--batch-id', help='Optional batch identifier')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Initialize ETL pipeline
    etl = TMDBETL()
    
    # Execute command
    if args.command == 'init':
        logger.info(f"Performing initial load from {args.start_year} to {args.end_year}")
        etl.initial_load(start_year=args.start_year, end_year=args.end_year)
    elif args.command == 'changes':
        logger.info(f"Processing changes from the last {args.days} days")
        etl.process_changes(days=args.days)
    elif args.command == 'missing':
        after_date = None
        if args.after_date:
            after_date = datetime.strptime(args.after_date, '%Y-%m-%d')
        logger.info(f"Getting missing movies after {after_date.date() if after_date else 'N/A'}")
        etl.process_missing_movies(after_date=after_date)
    elif args.command == 'search':
        logger.info(f"Searching for movie by {args.by}: {args.query}")
        etl.search_movie(args.query, args.by)
    elif args.command == 'fetch':
        etl.fetch_to_yaml(args.type, args.batch_id)
    elif args.command == 'review':
        etl.review_yaml(args.file)
    elif args.command == 'load':
        etl.load_from_yaml(args.file)
    elif args.command == 'fetch-review-load':
        etl.fetch_review_load(args.type, args.batch_id)
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 