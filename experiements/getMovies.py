import requests
import os
import yaml
from dotenv import load_dotenv
from datetime import datetime
from tqdm import tqdm

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.themoviedb.org/3"

class Movie:
    def __init__(self, data):
        self.id = data.get("id")
        self.title = data.get("title")
        self.release_date = data.get("release_date")
        self.popularity = data.get("popularity")
        self.vote_average = data.get("vote_average")
        self.overview = data.get("overview")
        # Add more fields as needed

    def __repr__(self):
        return f"<Movie {self.title} ({self.release_date})>"

    def to_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            "release_date": self.release_date,
            "popularity": self.popularity,
            "vote_average": self.vote_average,
            "overview": self.overview
        }

class MovieCollection:
    def __init__(self, movies):
        self.movies = movies

    def filter_by_year(self, year):
        return [m for m in self.movies if m.release_date and m.release_date.startswith(str(year))]

    def top_rated(self, n=10):
        return sorted(self.movies, key=lambda m: m.vote_average or 0, reverse=True)[:n]

    def to_yaml(self, filename):
        with open(filename, "w") as f:
            yaml.dump([m.to_dict() for m in self.movies], f, default_flow_style=False)

def get_movies_by_year(year, page=1):
    url = f"{BASE_URL}/discover/movie"
    params = {
        "api_key": API_KEY,
        "sort_by": "popularity.desc",
        "page": page,
        "primary_release_year": year
    }
    response = requests.get(url, params=params)
    return response.json()

def fetch_all_movies_by_year(year):
    movies = []
    first_page = get_movies_by_year(year, 1)
    total_pages = min(first_page.get("total_pages", 1), 500)  # TMDB API max is 500
    movies.extend(first_page.get("results", []))
    for page in tqdm(range(2, total_pages + 1), desc=f"Fetching movies for {year}"):
        data = get_movies_by_year(year, page)
        movies.extend(data.get("results", []))
    return movies

if __name__ == "__main__":
    current_year = datetime.now().year
    os.makedirs("movies_data", exist_ok=True)
    for year in range(1900, current_year + 1):
        raw_movies = fetch_all_movies_by_year(year)
        movie_objects = [Movie(data) for data in raw_movies]
        collection = MovieCollection(movie_objects)
        print(f"Retrieved {len(collection.movies)} movies for the year {year}.")
        collection.to_yaml(f"movies_data/movies_{year}.yaml")
        print(f"Movies saved to movies_data/movies_{year}.yaml.")
