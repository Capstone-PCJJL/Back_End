import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
from tqdm import tqdm
from fuzzywuzzy import process
import sys
import time

from src.api.tmdb_client import TMDBClient
from src.database.db_manager import DatabaseManager
from sqlalchemy import text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TMDBUpdater:
    def __init__(self):
        """Initialize TMDB updater."""
        self.client = TMDBClient()
        self.db = DatabaseManager()
        self.conn = self.db.engine.connect()

    def update_existing_movie(self, movie_id: int) -> bool:
        """Update an existing movie's information in the database."""
        try:
            # Get movie details from TMDB
            movie_data = self.client.get_movie_details(movie_id)
            if not movie_data:
                logger.error(f"Could not find movie with ID {movie_id} in TMDB")
                return False

            # Log genres before update
            genres = [genre['name'] for genre in movie_data.get('genres', [])]
            logger.info(f"Updating movie {movie_id} with genres: {', '.join(genres) if genres else 'No genres'}")

            # Get credits
            credits_data = self.client.get_movie_credits(movie_id)
            if not credits_data:
                logger.warning(f"No credits found for movie {movie_id}")
                return False

            # Update movie record
            update_stmt = text("""
                UPDATE movies 
                SET title = :title,
                    original_title = :original_title,
                    overview = :overview,
                    release_date = :release_date,
                    runtime = :runtime,
                    status = :status,
                    vote_average = :vote_average,
                    vote_count = :vote_count,
                    popularity = :popularity,
                    poster_path = :poster_path,
                    backdrop_path = :backdrop_path,
                    budget = :budget,
                    revenue = :revenue,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
            """)

            # Prepare movie data
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

            # Execute update
            self.conn.execute(update_stmt, movie_record)

            # Update genres
            self._update_genres(movie_id, movie_data)

            # Update credits
            self._update_credits(movie_id, credits_data)

            self.conn.commit()
            logger.info(f"Successfully updated movie {movie_id}")
            return True

        except Exception as e:
            logger.error(f"Error updating movie {movie_id}: {str(e)}")
            return False

    def _update_credits(self, movie_id: int, credits_data: Dict):
        """Update credits for a movie."""
        try:
            # Delete existing credits
            delete_stmt = text("DELETE FROM credits WHERE movie_id = :movie_id")
            self.conn.execute(delete_stmt, {'movie_id': movie_id})

            # Prepare credits records
            credits_records = []

            # Process cast (actors)
            actors = credits_data.get('cast', [])[:8]  # Top 8 actors
            if actors:
                logger.info(f"Processing {len(actors)} actors for movie {movie_id}")
                for person in actors:
                    if person.get('id'):
                        credits_records.append({
                            'movie_id': movie_id,
                            'person_id': person['id'],
                            'credit_type': 'cast',
                            'character_name': person.get('character'),
                            'credit_order': person.get('order'),
                            'department': 'Acting',
                            'job': 'Actor'
                        })
                        logger.info(f"  - Actor: {person.get('name')} as {person.get('character', 'Unknown')}")
            else:
                logger.warning(f"No actors found for movie {movie_id}")

            # Process crew (directors only)
            directors = [person for person in credits_data.get('crew', [])
                        if person.get('job') == 'Director'][:1]  # Only get the first director
            if directors:
                director = directors[0]
                if director.get('id'):
                    credits_records.append({
                        'movie_id': movie_id,
                        'person_id': director['id'],
                        'credit_type': 'crew',
                        'character_name': None,
                        'credit_order': None,
                        'department': director.get('department', 'Directing'),
                        'job': director.get('job')
                    })
                    logger.info(f"  - Director: {director.get('name')}")
            else:
                logger.warning(f"No director found for movie {movie_id}")

            if not credits_records:
                logger.warning(f"No credits found for movie {movie_id}")
                return

            # Insert new credits
            insert_stmt = text("""
                INSERT INTO credits (
                    movie_id, person_id, credit_type, character_name,
                    credit_order, department, job
                ) VALUES (
                    :movie_id, :person_id, :credit_type, :character_name,
                    :credit_order, :department, :job
                )
            """)

            self.conn.execute(insert_stmt, credits_records)
            self.conn.commit()

            # Log summary
            actor_count = len([r for r in credits_records if r['credit_type'] == 'cast'])
            director_count = len([r for r in credits_records if r['credit_type'] == 'crew'])
            logger.info(f"Added {actor_count} actors and {director_count} director(s) for movie {movie_id}")

        except Exception as e:
            logger.error(f"Error updating credits for movie {movie_id}: {str(e)}")
            raise

    def _update_genres(self, movie_id: int, movie_data: Dict):
        """Update genres for a movie."""
        try:
            # Delete existing genres
            delete_stmt = text("DELETE FROM genres WHERE movie_id = :movie_id")
            self.conn.execute(delete_stmt, {'movie_id': movie_id})

            # Get genres from movie data
            genres = movie_data.get('genres', [])
            if not genres:
                logger.warning(f"No genres found for movie {movie_id}")
                return

            # Insert new genres
            insert_stmt = text("""
                INSERT INTO genres (movie_id, genre_name)
                VALUES (:movie_id, :genre_name)
            """)

            genre_records = [
                {'movie_id': movie_id, 'genre_name': genre['name']}
                for genre in genres
            ]

            self.conn.execute(insert_stmt, genre_records)
            self.conn.commit()
            
            # Log genre names
            genre_names = [genre['name'] for genre in genres]
            logger.info(f"Added genres for movie {movie_id}: {', '.join(genre_names)}")

        except Exception as e:
            logger.error(f"Error updating genres for movie {movie_id}: {str(e)}")
            raise

    def search_and_add_movie(self, search_term: str) -> bool:
        """Search for a movie and add it to the database."""
        try:
            # Check if search_term is a movie ID
            if search_term.isdigit():
                movie_id = int(search_term)
                # Check if movie exists in TMDB
                movie_data = self.client.get_movie_details(movie_id)
                if not movie_data:
                    logger.error(f"Could not find movie with ID {movie_id} in TMDB")
                    return False
                
                # Check if movie already exists in database
                existing_ids = self._get_existing_movie_ids()
                if movie_id in existing_ids:
                    logger.info(f"Movie with ID {movie_id} already exists in database")
                    return False
                
                # Add movie directly
                return self._add_movie_to_db(movie_id)
            
            # If not a movie ID, search by name
            search_results = self.client.search_movie(search_term)
            if not search_results:
                logger.error(f"No results found for '{search_term}'")
                return False

            # Get existing movie IDs from database
            existing_ids = self._get_existing_movie_ids()

            # Filter out existing movies
            new_results = [movie for movie in search_results if movie['id'] not in existing_ids]

            if not new_results:
                logger.info("All found movies already exist in database")
                return False

            # Get detailed movie info including genres for each result
            detailed_results = []
            for movie in new_results:
                movie_id = movie['id']
                movie_details = self.client.get_movie_details(movie_id)
                if movie_details:
                    detailed_results.append(movie_details)

            # Display results for user selection
            print("\nFound the following movies:")
            for i, movie in enumerate(detailed_results, 1):
                release_date = movie.get('release_date', 'N/A')
                genres = [genre['name'] for genre in movie.get('genres', [])]
                genres_str = ', '.join(genres) if genres else 'No genres'
                print(f"{i}. {movie['title']} ({release_date}) - ID: {movie['id']}")
                print(f"   Genres: {genres_str}")

            # Get user selection
            while True:
                try:
                    selection = int(input("\nEnter the number of the movie to add (0 to cancel): "))
                    if selection == 0:
                        return False
                    if 1 <= selection <= len(detailed_results):
                        break
                    print("Invalid selection. Please try again.")
                except ValueError:
                    print("Please enter a valid number.")

            # Get selected movie
            selected_movie = detailed_results[selection - 1]
            
            # Add movie to database
            return self._add_movie_to_db(selected_movie['id'])

        except Exception as e:
            logger.error(f"Error searching and adding movie: {str(e)}")
            return False

    def _get_existing_movie_ids(self) -> List[int]:
        """Get list of existing movie IDs from database."""
        try:
            result = self.conn.execute(text("SELECT id FROM movies"))
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error getting existing movie IDs: {str(e)}")
            return []

    def _add_movie_to_db(self, movie_id: int) -> bool:
        """Add a new movie to the database."""
        try:
            # Check if movie already exists
            existing_ids = self._get_existing_movie_ids()
            if movie_id in existing_ids:
                logger.info(f"Movie with ID {movie_id} already exists in database")
                return False

            # Get movie details
            movie_data = self.client.get_movie_details(movie_id)
            if not movie_data:
                logger.error(f"Could not find movie with ID {movie_id} in TMDB")
                return False

            # Get credits
            credits_data = self.client.get_movie_credits(movie_id)
            if not credits_data:
                logger.warning(f"No credits found for movie {movie_id} - will add movie without credits")

            # Insert movie
            insert_stmt = text("""
                INSERT INTO movies (
                    id, title, original_title, overview, release_date, runtime,
                    status, vote_average, vote_count, popularity, poster_path,
                    backdrop_path, budget, revenue
                ) VALUES (
                    :id, :title, :original_title, :overview, :release_date, :runtime,
                    :status, :vote_average, :vote_count, :popularity, :poster_path,
                    :backdrop_path, :budget, :revenue
                )
            """)

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

            self.conn.execute(insert_stmt, movie_record)

            # Add genres
            self._update_genres(movie_id, movie_data)

            # Add credits if available
            if credits_data:
                self._update_credits(movie_id, credits_data)
                logger.info(f"Successfully added movie {movie_id} with credits")
            else:
                logger.info(f"Successfully added movie {movie_id} without credits")

            self.conn.commit()
            return True

        except Exception as e:
            logger.error(f"Error adding movie {movie_id}: {str(e)}")
            return False

    def add_new_movies(self, time_period: str = None) -> int:
        """Add new movies to the database based on release date."""
        try:
            # Get the latest movie date from database
            result = self.conn.execute(text("""
                SELECT MAX(release_date) FROM movies 
                WHERE release_date IS NOT NULL
            """))
            latest_date = result.scalar()

            if not latest_date:
                logger.error("No movies found in database")
                return 0

            # Calculate start date based on time period
            if time_period:
                if time_period == 'day':
                    start_date = latest_date - timedelta(days=1)
                elif time_period == 'week':
                    start_date = latest_date - timedelta(weeks=1)
                elif time_period == 'month':
                    start_date = latest_date - timedelta(days=30)
                else:
                    logger.error(f"Invalid time period: {time_period}")
                    return 0
            else:
                start_date = latest_date

            # Get new movies from TMDB
            new_movies = self.client.get_movies_since_date(start_date)
            if not new_movies:
                logger.info("No new movies found")
                return 0

            # Get existing movie IDs
            existing_ids = self._get_existing_movie_ids()

            # Filter out existing movies
            movies_to_add = [movie for movie in new_movies if movie['id'] not in existing_ids]

            # Add new movies
            added_count = 0
            for movie in tqdm(movies_to_add, desc="Adding new movies"):
                # Get detailed movie info including genres
                movie_details = self.client.get_movie_details(movie['id'])
                if not movie_details:
                    continue

                # Log genres for this movie
                genres = [genre['name'] for genre in movie_details.get('genres', [])]
                logger.info(f"Adding new movie {movie['id']} with genres: {', '.join(genres) if genres else 'No genres'}")

                if self._add_movie_to_db(movie['id']):
                    added_count += 1

            logger.info(f"Added {added_count} new movies")
            return added_count

        except Exception as e:
            logger.error(f"Error adding new movies: {str(e)}")
            return 0

    def update_all_movies(self, batch_size: int = 100) -> int:
        """Update all movies in the database that have been updated in TMDB."""
        try:
            # Get total count of movies
            count_result = self.conn.execute(text("SELECT COUNT(*) FROM movies"))
            total_movies = count_result.scalar()

            if total_movies == 0:
                logger.info("No movies found in database")
                return 0

            updated_count = 0
            processed_count = 0

            # Process movies in batches
            while processed_count < total_movies:
                # Get batch of movies
                result = self.conn.execute(text("""
                    SELECT id, updated_at 
                    FROM movies 
                    ORDER BY updated_at ASC
                    LIMIT :limit OFFSET :offset
                """), {
                    'limit': batch_size,
                    'offset': processed_count
                })
                
                batch_movies = [(row[0], row[1]) for row in result]
                
                if not batch_movies:
                    break

                logger.info(f"Processing batch of {len(batch_movies)} movies "
                          f"({processed_count + 1} to {processed_count + len(batch_movies)} "
                          f"of {total_movies})")

                for movie_id, last_update in tqdm(batch_movies, desc="Checking for updates"):
                    try:
                        # Get movie details from TMDB
                        movie_data = self.client.get_movie_details(movie_id)
                        if not movie_data:
                            logger.warning(f"Could not find movie {movie_id} in TMDB")
                            continue

                        # Log genres for this movie
                        genres = [genre['name'] for genre in movie_data.get('genres', [])]
                        logger.info(f"Updating movie {movie_id} with genres: {', '.join(genres) if genres else 'No genres'}")

                        # Update movie regardless of last update time
                        if self.update_existing_movie(movie_id):
                            updated_count += 1

                    except Exception as e:
                        logger.error(f"Error updating movie {movie_id}: {str(e)}")
                        continue

                processed_count += len(batch_movies)
                
                # Log progress
                logger.info(f"Processed {processed_count}/{total_movies} movies. "
                          f"Updated {updated_count} movies so far.")

                # Add a small delay between batches to avoid rate limits
                if processed_count < total_movies:
                    logger.info("Waiting 1 second before processing next batch...")
                    time.sleep(1)

            logger.info(f"Completed processing all movies. Updated {updated_count} movies in total.")
            return updated_count

        except Exception as e:
            logger.error(f"Error updating all movies: {str(e)}")
            return 0

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='TMDB Data Update Tool')
    parser.add_argument('--update', type=int, nargs='?', const=True,
                       help='Update movie by ID or all movies if no ID provided')
    parser.add_argument('--search', type=str, help='Search and add movie by name')
    parser.add_argument('--add-new-movies', action='store_true', help='Add new movies since last update')
    parser.add_argument('--time-period', choices=['day', 'week', 'month'], 
                       help='Time period for new movies (day/week/month)')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Number of movies to process in each batch (default: 100)')
    
    args = parser.parse_args()
    
    updater = TMDBUpdater()
    
    if args.update is not None:
        if args.update is True:  # No ID provided
            updater.update_all_movies(batch_size=args.batch_size)
        else:  # ID provided
            updater.update_existing_movie(args.update)
    elif args.search:
        updater.search_and_add_movie(args.search)
    elif args.add_new_movies:
        updater.add_new_movies(args.time_period)
    else:
        parser.print_help()

if __name__ == '__main__':
    main() 