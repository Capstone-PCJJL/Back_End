import yaml
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class YAMLHandler:
    """Handles YAML file operations for movie data storage."""
    
    def __init__(self, base_dir: str = "data/movies"):
        """
        Initialize YAML handler.
        
        Args:
            base_dir: Base directory for storing YAML files
        """
        self.base_dir = base_dir
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """Ensure required directories exist."""
        os.makedirs(self.base_dir, exist_ok=True)
        os.makedirs(os.path.join(self.base_dir, "raw"), exist_ok=True)
        os.makedirs(os.path.join(self.base_dir, "processed"), exist_ok=True)
    
    def save_movie_batch(self, movies: List[Dict[str, Any]], batch_type: str, batch_id: str) -> str:
        """
        Save a batch of movies to a YAML file.
        
        Args:
            movies: List of movie data dictionaries
            batch_type: Type of batch ('initial', 'missing', 'changes')
            batch_id: Unique identifier for the batch
            
        Returns:
            str: Path to the saved YAML file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{batch_type}_{batch_id}_{timestamp}.yaml"
        filepath = os.path.join(self.base_dir, "raw", filename)
        
        data = {
            "metadata": {
                "batch_type": batch_type,
                "batch_id": batch_id,
                "timestamp": timestamp,
                "movie_count": len(movies)
            },
            "movies": movies
        }
        
        try:
            with open(filepath, 'w') as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False)
            logger.info(f"Saved {len(movies)} movies to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error saving YAML file: {str(e)}")
            raise
    
    def load_movie_batch(self, filepath: str) -> Dict[str, Any]:
        """
        Load a batch of movies from a YAML file.
        
        Args:
            filepath: Path to the YAML file
            
        Returns:
            Dict containing metadata and movies
        """
        try:
            with open(filepath, 'r') as f:
                data = yaml.safe_load(f)
            logger.info(f"Loaded {data['metadata']['movie_count']} movies from {filepath}")
            return data
        except Exception as e:
            logger.error(f"Error loading YAML file: {str(e)}")
            raise
    
    def mark_as_processed(self, filepath: str) -> None:
        """
        Move a processed YAML file to the processed directory.
        
        Args:
            filepath: Path to the YAML file
        """
        try:
            filename = os.path.basename(filepath)
            new_path = os.path.join(self.base_dir, "processed", filename)
            os.rename(filepath, new_path)
            logger.info(f"Moved {filepath} to processed directory")
        except Exception as e:
            logger.error(f"Error moving processed file: {str(e)}")
            raise
    
    def get_unprocessed_files(self, batch_type: Optional[str] = None) -> List[str]:
        """
        Get list of unprocessed YAML files.
        
        Args:
            batch_type: Optional filter for batch type
            
        Returns:
            List of file paths
        """
        raw_dir = os.path.join(self.base_dir, "raw")
        files = []
        
        for filename in os.listdir(raw_dir):
            if filename.endswith('.yaml'):
                if batch_type is None or filename.startswith(batch_type):
                    files.append(os.path.join(raw_dir, filename))
        
        return sorted(files)
    
    def get_movie_schema(self) -> Dict[str, Any]:
        """
        Get the schema for movie data.
        
        Returns:
            Dictionary containing the schema definition
        """
        return {
            "schema_version": "1.0",
            "movie_schema": {
                "tmdb_id": "integer",
                "movielens_id": "integer",
                "title": "string",
                "original_title": "string",
                "release_date": "datetime",
                "overview": "text",
                "poster_path": "string",
                "backdrop_path": "string",
                "adult": "boolean",
                "original_language": "string",
                "runtime": "integer",
                "status": "string",
                "tagline": "string",
                "popularity": "float",
                "vote_average": "float",
                "vote_count": "integer",
                "movielens_rating": "float",
                "movielens_num_ratings": "integer",
                "movielens_tags": "list",
                "genres": "list",
                "credits": {
                    "cast": "list",
                    "crew": "list"
                },
                "keywords": "list"
            }
        } 