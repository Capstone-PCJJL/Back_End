import os
import yaml
from datetime import datetime
from typing import List, Dict, Any
from pathlib import Path

class DataStorage:
    """Handles storage of movie data in various formats."""
    
    def __init__(self, base_dir: str = "data"):
        """
        Initialize the data storage handler.
        
        Args:
            base_dir: Base directory for storing data
        """
        self.base_dir = Path(base_dir)
        self.movies_dir = self.base_dir / "movies"
        self.changes_dir = self.base_dir / "changes"
        
        # Create directories
        self.movies_dir.mkdir(parents=True, exist_ok=True)
        self.changes_dir.mkdir(parents=True, exist_ok=True)

    def save_movies_by_year(self, movies: List[Dict[str, Any]], year: int) -> None:
        """
        Save movies for a specific year to YAML file.
        
        Args:
            movies: List of movie dictionaries
            year: Year to save movies for
        """
        # Add image URLs to each movie
        for movie in movies:
            # Handle poster image
            if movie.get("poster_path"):
                movie["poster_url"] = self.get_movie_image_url(movie["poster_path"])
            else:
                movie["poster_url"] = None
                movie["poster_path"] = None

            # Handle backdrop image
            if movie.get("backdrop_path"):
                movie["backdrop_url"] = self.get_movie_image_url(movie["backdrop_path"], "original")
            else:
                movie["backdrop_url"] = None
                movie["backdrop_path"] = None

        filename = self.movies_dir / f"movies_{year}.yaml"
        with open(filename, "w") as f:
            yaml.dump(movies, f, default_flow_style=False, sort_keys=False)
        print(f"Movies saved to {filename}")

    def save_movie_changes(self, changes: Dict[str, Any]) -> None:
        """
        Save movie changes to YAML file.
        
        Args:
            changes: Movie changes data
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.changes_dir / f"changes_{timestamp}.yaml"
        with open(filename, "w") as f:
            yaml.dump(changes, f, default_flow_style=False, sort_keys=False)
        print(f"Changes saved to {filename}")

    def get_movie_image_url(self, poster_path: str, size: str = "w500") -> str:
        """
        Get the full URL for a movie image.
        
        Args:
            poster_path: Path to the image
            size: Image size (w92, w154, w185, w342, w500, w780, original)
            
        Returns:
            str: Full URL to the image
        """
        if not poster_path:
            return None
        return f"https://image.tmdb.org/t/p/{size}{poster_path}" 