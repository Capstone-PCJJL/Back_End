import requests
import os
from dotenv import load_dotenv

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

class MovieCollection:
    def __init__(self, movies):
        self.movies = movies

    def filter_by_year(self, year):
        return [m for m in self.movies if m.release_date and m.release_date.startswith(str(year))]

    def top_rated(self, n=10):
        return sorted(self.movies, key=lambda m: m.vote_average or 0, reverse=True)[:n]

def get_movies(page=1):
    url = f"{BASE_URL}/discover/movie"
    params = {
        "api_key": API_KEY,
        "sort_by": "popularity.desc",
        "page": page
    }
    response = requests.get(url, params=params)
    return response.json()

def fetch_all_movies():
    movies = []
    first_page = get_movies(1)
    total_pages = min(first_page.get("total_pages", 1), 500)  # TMDB API max is 500
    movies.extend(first_page.get("results", []))
    for page in range(2, total_pages + 1):
        data = get_movies(page)
        movies.extend(data.get("results", []))
    return movies

if __name__ == "__main__":
    raw_movies = fetch_all_movies()
    movie_objects = [Movie(data) for data in raw_movies]
    collection = MovieCollection(movie_objects)
    print(f"Retrieved {len(collection.movies)} movies.")
    print("Top 5 rated movies:", collection.top_rated(5))
