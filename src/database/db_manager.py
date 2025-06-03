from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Boolean, DateTime, ForeignKey, JSON, Text
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import logging
from sqlalchemy.sql import text

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base = declarative_base()

class Movie(Base):
    """Movie table model."""
    __tablename__ = 'movies'

    id = Column(Integer, primary_key=True)
    tmdb_id = Column(Integer, unique=True, nullable=False)
    movielens_id = Column(Integer, unique=True)
    title = Column(String(255), nullable=False)
    original_title = Column(String(255))
    release_date = Column(DateTime)
    overview = Column(Text)
    poster_path = Column(String(255))
    backdrop_path = Column(String(255))
    adult = Column(Boolean, default=False)
    original_language = Column(String(10))
    runtime = Column(Integer)
    status = Column(String(50))  # Rumored, Planned, In Production, Post Production, Released, Canceled
    tagline = Column(String(255))
    popularity = Column(Float)
    vote_average = Column(Float)
    vote_count = Column(Integer)
    movielens_rating = Column(Float)
    movielens_num_ratings = Column(Integer)
    movielens_tags = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    genres = relationship("MovieGenre", back_populates="movie")
    credits = relationship("MovieCredit", back_populates="movie")
    keywords = relationship("MovieKeyword", back_populates="movie")

class Genre(Base):
    """Genre table model."""
    __tablename__ = 'genres'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)

    # Relationships
    movies = relationship("MovieGenre", back_populates="genre")

class MovieGenre(Base):
    """Movie-Genre relationship table."""
    __tablename__ = 'movie_genres'

    id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, ForeignKey('movies.id'), nullable=False)
    genre_id = Column(Integer, ForeignKey('genres.id'), nullable=False)

    # Relationships
    movie = relationship("Movie", back_populates="genres")
    genre = relationship("Genre", back_populates="movies")

class Person(Base):
    """Person table model (actors, directors, crew)."""
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    tmdb_id = Column(Integer, unique=True, nullable=False)
    name = Column(String(255), nullable=False)
    profile_path = Column(String(255))
    gender = Column(Integer)  # 0: Not specified, 1: Female, 2: Male
    known_for_department = Column(String(100))

    # Relationships
    credits = relationship("MovieCredit", back_populates="person")

class MovieCredit(Base):
    """Movie credits table (cast and crew)."""
    __tablename__ = 'movie_credits'

    id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, ForeignKey('movies.id'), nullable=False)
    person_id = Column(Integer, ForeignKey('people.id'), nullable=False)
    credit_type = Column(String(50))  # 'cast' or 'crew'
    character = Column(String(255))  # for cast
    order = Column(Integer)  # for cast
    department = Column(String(100))  # for crew
    job = Column(String(100))  # for crew

    # Relationships
    movie = relationship("Movie", back_populates="credits")
    person = relationship("Person", back_populates="credits")

class MovieKeyword(Base):
    """Movie keywords table."""
    __tablename__ = 'movie_keywords'

    id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, ForeignKey('movies.id'), nullable=False)
    tmdb_id = Column(Integer)
    name = Column(String(100))

    # Relationships
    movie = relationship("Movie", back_populates="keywords")

class MovieChange(Base):
    """Movie changes tracking table."""
    __tablename__ = 'movie_changes'

    id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, ForeignKey('movies.id'), nullable=False)
    change_type = Column(String(50), nullable=False)  # 'created', 'updated', 'deleted'
    change_date = Column(DateTime, default=datetime.utcnow)
    details = Column(JSON)

class DatabaseManager:
    """Manages database operations for the movie data system."""
    
    def __init__(self):
        """Initialize the database connection."""
        load_dotenv()
        
        # Get database configuration from environment variables
        db_host = os.getenv("SQL_HOST", "localhost")
        db_port = os.getenv("SQL_PORT", "3306")
        db_user = os.getenv("SQL_USER")
        db_pass = os.getenv("SQL_PASS")
        db_name = os.getenv("SQL_DB")
        
        if not all([db_user, db_pass, db_name]):
            raise ValueError("Database credentials not found in environment variables")
        
        # Create database URL
        db_url = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        
        # Create engine and session
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()  # Create a session instance
        
        # Store model classes for easy access
        self.Movie = Movie
        self.Person = Person
        self.Genre = Genre
        self.MovieGenre = MovieGenre
        self.MovieCredit = MovieCredit
        self.MovieKeyword = MovieKeyword
        self.MovieChange = MovieChange

    def __del__(self):
        """Clean up the session when the object is destroyed."""
        if hasattr(self, 'session'):
            self.session.close()

    def create_tables(self) -> None:
        """Create all database tables."""
        try:
            # Get database name from engine URL
            db_name = self.engine.url.database
            
            # Drop all tables using raw SQL
            with self.engine.connect() as connection:
                # Disable foreign key checks
                connection.execute(text("SET FOREIGN_KEY_CHECKS=0"))
                
                # Get all table names
                result = connection.execute(text(f"""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = '{db_name}'
                """))
                tables = [row[0] for row in result]
                
                # Drop all tables
                for table in tables:
                    try:
                        connection.execute(text(f"DROP TABLE IF EXISTS `{table}`"))
                    except Exception as e:
                        logger.warning(f"Warning dropping table {table}: {str(e)}")
                
                # Re-enable foreign key checks
                connection.execute(text("SET FOREIGN_KEY_CHECKS=1"))
                
                # Commit the transaction
                connection.commit()
            
            logger.info("✅ Existing tables dropped successfully")
            
            # Create all tables
            Base.metadata.create_all(self.engine)
            logger.info("✅ Database tables created successfully")
        except Exception as e:
            logger.error(f"❌ Error creating database tables: {str(e)}")
            raise

    def clear_database(self) -> None:
        """
        Clear all data from the database tables.
        This will remove all records while keeping the table structure intact.
        """
        try:
            # Get all tables in reverse order of dependencies
            tables = [
                self.MovieKeyword,
                self.MovieCredit,
                self.MovieGenre,
                self.MovieChange,
                self.Movie,
                self.Person,
                self.Genre
            ]
            
            # Delete data from each table
            for table in tables:
                self.session.query(table).delete()
            
            self.session.commit()
            logger.info("✅ Database cleared successfully")
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error clearing database: {str(e)}")
            raise

    def load_movie_data(self, movie_data: Dict[str, Any]) -> None:
        """
        Load movie data into the database.
        
        Args:
            movie_data: Dictionary containing movie data
        """
        try:
            session = self.Session()
            
            # Get TMDB ID from either field name
            tmdb_id = movie_data.get('tmdbId') or movie_data.get('tmdb_id')
            if not tmdb_id:
                raise ValueError("TMDB ID is required")
            
            # Check if movie already exists by TMDB ID
            existing_movie = session.query(self.Movie).filter_by(tmdb_id=tmdb_id).first()
            
            if existing_movie:
                # Update existing movie
                existing_movie.title = movie_data.get('title')
                existing_movie.original_title = movie_data.get('original_title')
                existing_movie.release_date = movie_data.get('release_date')
                existing_movie.overview = movie_data.get('overview')
                existing_movie.poster_path = movie_data.get('poster_path')
                existing_movie.backdrop_path = movie_data.get('backdrop_path')
                existing_movie.adult = movie_data.get('adult', False)
                existing_movie.original_language = movie_data.get('original_language')
                existing_movie.runtime = movie_data.get('runtime')
                existing_movie.status = movie_data.get('status')
                existing_movie.tagline = movie_data.get('tagline')
                existing_movie.popularity = movie_data.get('popularity')
                existing_movie.vote_average = movie_data.get('vote_average')
                existing_movie.vote_count = movie_data.get('vote_count')
                existing_movie.movielens_rating = movie_data.get('movielens_rating') or movie_data.get('average_rating')
                existing_movie.movielens_num_ratings = movie_data.get('movielens_num_ratings') or movie_data.get('num_ratings')
                existing_movie.movielens_tags = movie_data.get('movielens_tags') or movie_data.get('tags', [])
                existing_movie.updated_at = datetime.utcnow()
                
                # Track change
                self._track_change(session, existing_movie.id, 'update')
            else:
                # Create new movie
                movie = self.Movie(
                    tmdb_id=tmdb_id,
                    movielens_id=movie_data.get('movielens_id') or movie_data.get('movieId'),
                    title=movie_data.get('title'),
                    original_title=movie_data.get('original_title'),
                    release_date=movie_data.get('release_date'),
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
                    movielens_rating=movie_data.get('movielens_rating') or movie_data.get('average_rating'),
                    movielens_num_ratings=movie_data.get('movielens_num_ratings') or movie_data.get('num_ratings'),
                    movielens_tags=movie_data.get('movielens_tags') or movie_data.get('tags', [])
                )
                session.add(movie)
                session.flush()  # Get the movie ID
                
                # Track change
                self._track_change(session, movie.id, 'create')
            
            # Process genres
            if 'genres' in movie_data:
                # First, remove existing genre associations
                if existing_movie:
                    session.query(self.MovieGenre).filter_by(movie_id=existing_movie.id).delete()
                
                for genre_name in movie_data['genres']:
                    if genre_name == '(no genres listed)':
                        continue
                        
                    # Get or create genre
                    genre = session.query(self.Genre).filter_by(name=genre_name).first()
                    if not genre:
                        genre = self.Genre(name=genre_name)
                        session.add(genre)
                        session.flush()
                    
                    # Create movie-genre relationship
                    movie_genre = self.MovieGenre(
                        movie_id=existing_movie.id if existing_movie else movie.id,
                        genre_id=genre.id
                    )
                    session.add(movie_genre)
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error loading movie data: {str(e)}")
            logger.error(f"Movie data that caused error: {movie_data}")
            raise
        finally:
            session.close()

    def _get_or_create_person(self, person_data: Dict[str, Any]) -> Optional[Person]:
        """Get or create a person record."""
        try:
            person = self.session.query(Person).filter_by(tmdb_id=person_data['id']).first()
            if not person:
                person = Person(
                    tmdb_id=person_data['id'],
                    name=person_data['name'],
                    profile_path=person_data.get('profile_path'),
                    gender=person_data.get('gender'),
                    known_for_department=person_data.get('known_for_department')
                )
                self.session.add(person)
                self.session.flush()
            return person
        except Exception as e:
            logger.error(f"Error creating person: {str(e)}")
            return None

    def get_movie_by_tmdb_id(self, tmdb_id: int) -> Optional[Movie]:
        """
        Get movie by TMDB ID.
        
        Args:
            tmdb_id: TMDB movie ID
            
        Returns:
            Optional[Movie]: Movie object if found, None otherwise
        """
        try:
            return self.session.query(Movie).filter_by(tmdb_id=tmdb_id).first()
        except Exception as e:
            logger.error(f"Error getting movie by TMDB ID: {str(e)}")
            return None

    def get_recent_changes(self, days: int = 7) -> List[MovieChange]:
        """
        Get recent movie changes.
        
        Args:
            days: Number of days to look back
            
        Returns:
            List[MovieChange]: List of recent movie changes
        """
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            return self.session.query(MovieChange).filter(MovieChange.change_date >= cutoff_date).all()
        except Exception as e:
            logger.error(f"Error getting recent changes: {str(e)}")
            return []

    def _track_change(self, session, movie_id: int, change_type: str, details: Optional[Dict] = None) -> None:
        """Track changes made to movies in the database.
        
        Args:
            session: SQLAlchemy session to use
            movie_id: The ID of the movie that was changed
            change_type: Type of change ('created', 'updated', 'deleted')
            details: Optional dictionary with additional change details
        """
        change = self.MovieChange(
            movie_id=movie_id,
            change_type=change_type,
            details=details
        )
        session.add(change)
        # Do not commit here; commit is handled by the caller 

    def load_movie_data_batch(self, movie_data_list: List[Dict[str, Any]]) -> None:
        """
        Load a batch of movie data into the database.
        
        Args:
            movie_data_list: List of movie data dictionaries
        """
        session = self.Session()
        try:
            # Get all existing movies in bulk
            tmdb_ids = [movie_data.get('tmdb_id') or movie_data.get('tmdbId') for movie_data in movie_data_list]
    
            existing_movies = {
                movie.tmdb_id: movie 
                for movie in session.query(self.Movie).filter(self.Movie.tmdb_id.in_(tmdb_ids)).all()
            }

            # Prepare bulk insert/update operations
            movies_to_add = []
            movies_to_update = []
            
            for movie_data in movie_data_list:
                # Get TMDB ID from either field name
                tmdb_id = movie_data.get('tmdb_id') or movie_data.get('tmdbId')
                if not tmdb_id:
                    logger.warning(f"Skipping movie without TMDB ID: {movie_data}")
                    continue

                # Skip if movie already exists
                if tmdb_id in existing_movies:
                    logger.info(f"Movie with TMDB ID {tmdb_id} already exists, skipping...")
                    continue

                # Convert release_date string to datetime if present
                if movie_data.get('release_date'):
                    try:
                        movie_data['release_date'] = datetime.strptime(movie_data['release_date'], '%Y-%m-%d')
                    except ValueError:
                        movie_data['release_date'] = None
                
                # Create new movie
                movie = self.Movie(
                    tmdb_id=tmdb_id,
                    movielens_id=movie_data.get('movielens_id') or movie_data.get('movieId'),
                    title=movie_data.get('title'),
                    original_title=movie_data.get('original_title'),
                    release_date=movie_data.get('release_date'),
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
                    movielens_rating=movie_data.get('movielens_rating') or movie_data.get('average_rating'),
                    movielens_num_ratings=movie_data.get('movielens_num_ratings') or movie_data.get('num_ratings'),
                    movielens_tags=movie_data.get('movielens_tags') or movie_data.get('tags', [])
                )
                movies_to_add.append(movie)
            
            # Bulk save objects
            if movies_to_add:
                session.bulk_save_objects(movies_to_add)
            
            # Process genres in bulk
            for movie_data in movie_data_list:
                tmdb_id = movie_data.get('tmdb_id') or movie_data.get('tmdbId')
                if not tmdb_id or tmdb_id in existing_movies:
                    continue
                    
                movie = session.query(self.Movie).filter_by(tmdb_id=tmdb_id).first()
                
                if movie and movie.id and 'genres' in movie_data:
                    # Remove existing genre associations
                    session.query(self.MovieGenre).filter_by(movie_id=movie.id).delete()
                    
                    # Process new genres
                    for genre_name in movie_data.get('genres', []):
                        if genre_name == '(no genres listed)':
                            continue
                            
                        # Get or create genre
                        genre = session.query(self.Genre).filter_by(name=genre_name).first()
                        if not genre:
                            genre = self.Genre(name=genre_name)
                            session.add(genre)
                            session.flush()
                        
                        # Create movie-genre relationship
                        movie_genre = self.MovieGenre(
                            movie_id=movie.id,
                            genre_id=genre.id
                        )
                        session.add(movie_genre)
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error loading movie data batch: {str(e)}")
            raise
        finally:
            session.close()

    def get_latest_release_date(self) -> Optional[datetime]:
        """
        Get the most recent release date in the database.
        
        Returns:
            Optional[datetime]: The latest release date found, or None if no movies exist
        """
        try:
            session = self.Session()
            latest_movie = session.query(self.Movie)\
                .filter(self.Movie.release_date.isnot(None))\
                .order_by(self.Movie.release_date.desc())\
                .first()
            return latest_movie.release_date if latest_movie else None
        except Exception as e:
            logger.error(f"Error getting latest release date: {str(e)}")
            return None
        finally:
            session.close() 