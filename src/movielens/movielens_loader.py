import pandas as pd
import logging
from typing import Dict, List, Any, Optional, Iterator
from datetime import datetime
import os
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MovieLensLoader:
    """Loader for MovieLens ml-32m dataset."""
    
    def __init__(self, data_dir: str = "ml-32m", chunk_size: int = 10000):
        """
        Initialize the MovieLens loader.
        
        Args:
            data_dir: Directory containing the MovieLens dataset files
            chunk_size: Number of rows to process at once
        """
        self.data_dir = data_dir
        self.chunk_size = chunk_size
        self.movies_df = None
        self.links_df = None
        self.ratings_df = None
        self.tags_df = None
        self.ratings_by_movie = None
        self.tags_by_movie = None
        
    def load_data(self) -> None:
        """Load all MovieLens dataset files into memory and pre-index for fast access."""
        try:
            # Load all data files
            self.movies_df = pd.read_csv(os.path.join(self.data_dir, 'movies.csv'))
            logger.info(f"Loaded {len(self.movies_df)} movies")
            
            # Extract year from title and add as a column
            def extract_year(title):
                if '(' in title and ')' in title:
                    year_str = title[title.rfind('(')+1:title.rfind(')')]
                    try:
                        return int(year_str)
                    except ValueError:
                        return None
                return None
            self.movies_df['year'] = self.movies_df['title'].apply(extract_year)
            
            self.links_df = pd.read_csv(os.path.join(self.data_dir, 'links.csv'))
            logger.info(f"Loaded {len(self.links_df)} movie links")
            
            # Load ratings and tags into memory
            self.ratings_df = pd.read_csv(os.path.join(self.data_dir, 'ratings.csv'))
            logger.info(f"Loaded {len(self.ratings_df)} ratings")
            
            self.tags_df = pd.read_csv(os.path.join(self.data_dir, 'tags.csv'))
            logger.info(f"Loaded {len(self.tags_df)} tags")
            
            # Pre-index ratings and tags by movieId for fast lookup
            self.ratings_by_movie = self.ratings_df.groupby('movieId')['rating'].apply(list).to_dict()
            self.tags_by_movie = self.tags_df.groupby('movieId')['tag'].apply(list).to_dict()
        except Exception as e:
            logger.error(f"Error loading MovieLens data: {str(e)}")
            raise
    
    def _get_movie_ratings(self, movie_id: int) -> Dict[str, Any]:
        """
        Get ratings statistics for a movie.
        
        Args:
            movie_id: MovieLens movie ID
            
        Returns:
            Dict[str, Any]: Ratings statistics
        """
        try:
            ratings = self.ratings_by_movie.get(movie_id, [])
            if not ratings:
                return {'average_rating': None, 'num_ratings': 0}
            return {
                'average_rating': sum(ratings) / len(ratings),
                'num_ratings': len(ratings)
            }
        except Exception as e:
            logger.error(f"Error getting ratings for movie {movie_id}: {str(e)}")
            return {'average_rating': None, 'num_ratings': 0}
    
    def _get_movie_tags(self, movie_id: int) -> List[str]:
        """
        Get tags for a movie.
        
        Args:
            movie_id: MovieLens movie ID
            
        Returns:
            List[str]: List of tags
        """
        try:
            return self.tags_by_movie.get(movie_id, [])
        except Exception as e:
            logger.error(f"Error getting tags for movie {movie_id}: {str(e)}")
            return []
    
    def get_movie_data(self, movie_id: int) -> Optional[Dict[str, Any]]:
        """
        Get movie data including ratings and tags.
        
        Args:
            movie_id: MovieLens movie ID
            
        Returns:
            Optional[Dict[str, Any]]: Movie data if found, None otherwise
        """
        try:
            # Get basic movie info
            movie_row = self.movies_df.loc[self.movies_df['movieId'] == movie_id]
            if movie_row.empty:
                return None
            movie = movie_row.iloc[0]
            
            # Get ratings statistics
            ratings_stats = self._get_movie_ratings(movie_id)
            
            # Get tags
            tags = self._get_movie_tags(movie_id)
            
            # Get TMDB ID
            link_row = self.links_df.loc[self.links_df['movieId'] == movie_id]
            if link_row.empty:
                tmdb_id = None
            else:
                tmdb_id = link_row['tmdbId'].iloc[0]
            
            # Use pre-extracted year
            year = movie['year']
            
            # Parse genres
            genres = movie['genres'].split('|') if pd.notna(movie['genres']) else []
            
            return {
                'movieId': int(movie_id),
                'tmdbId': int(tmdb_id) if pd.notna(tmdb_id) else None,
                'title': movie['title'],
                'year': year,
                'genres': genres,
                'average_rating': ratings_stats['average_rating'],
                'num_ratings': ratings_stats['num_ratings'],
                'tags': [tag for tag in tags if isinstance(tag, str) and tag.strip()]
            }
            
        except (IndexError, KeyError):
            return None
        except Exception as e:
            logger.error(f"Error getting movie data for {movie_id}: {str(e)}")
            return None
    
    def get_movies_by_year(self, year: int) -> Iterator[Dict[str, Any]]:
        """
        Get movies released in a specific year.
        
        Args:
            year: Year to filter by
            
        Returns:
            Iterator[Dict[str, Any]]: Iterator of movie data
        """
        # Filter movies by year using the new column
        year_movies = self.movies_df[self.movies_df['year'] == year]
        
        for _, movie in year_movies.iterrows():
            movie_data = self.get_movie_data(movie['movieId'])
            if movie_data:
                yield movie_data
    
    def get_latest_movie_year(self) -> int:
        """
        Get the latest movie year from the dataset.
        
        Returns:
            int: Latest movie year found in the dataset
        """
        try:
            # Extract year from title (format: "Title (Year)")
            years = []
            for title in self.movies_df['title']:
                if '(' in title and ')' in title:
                    year_str = title[title.rfind('(')+1:title.rfind(')')]
                    try:
                        year = int(year_str)
                        years.append(year)
                    except ValueError:
                        continue
            
            if not years:
                return 1900  # Default to 1900 if no years found
                
            return max(years)
            
        except Exception as e:
            logger.error(f"Error getting latest movie year: {str(e)}")
            return 1900  # Default to 1900 on error 