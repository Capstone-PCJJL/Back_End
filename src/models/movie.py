from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime

@dataclass
class Movie:
    """Movie data model."""
    
    id: int
    title: str
    release_date: str
    popularity: float
    vote_average: float
    vote_count: int
    overview: str
    poster_path: Optional[str]
    backdrop_path: Optional[str]
    genre_ids: List[int]
    adult: bool
    original_language: str
    original_title: str

    @classmethod
    def from_api_data(cls, data: dict) -> 'Movie':
        """Create a Movie instance from API data."""
        return cls(
            id=data.get("id"),
            title=data.get("title"),
            release_date=data.get("release_date"),
            popularity=data.get("popularity", 0.0),
            vote_average=data.get("vote_average", 0.0),
            vote_count=data.get("vote_count", 0),
            overview=data.get("overview", ""),
            poster_path=data.get("poster_path"),
            backdrop_path=data.get("backdrop_path"),
            genre_ids=data.get("genre_ids", []),
            adult=data.get("adult", False),
            original_language=data.get("original_language", ""),
            original_title=data.get("original_title", "")
        )

    def to_dict(self) -> dict:
        """Convert movie data to dictionary."""
        return {
            "id": self.id,
            "title": self.title,
            "release_date": self.release_date,
            "popularity": self.popularity,
            "vote_average": self.vote_average,
            "vote_count": self.vote_count,
            "overview": self.overview,
            "poster_path": self.poster_path,
            "backdrop_path": self.backdrop_path,
            "genre_ids": self.genre_ids,
            "adult": self.adult,
            "original_language": self.original_language,
            "original_title": self.original_title
        }

    def get_year(self) -> int:
        """Get the release year of the movie."""
        try:
            return datetime.strptime(self.release_date, "%Y-%m-%d").year
        except (ValueError, TypeError):
            return 0

    def __repr__(self) -> str:
        """String representation of the movie."""
        return f"<Movie {self.title} ({self.release_date})>" 