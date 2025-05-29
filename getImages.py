import requests

url = "https://api.themoviedb.org/3/movie/132394/images"

headers = {
    "accept": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJiMDQ0NmRiMTU0MDVhYTcyMzIxYTFiZGM2Mjk1NDQxNyIsIm5iZiI6MTc0ODU0MzcwMi4zMDQ5OTk4LCJzdWIiOiI2ODM4YThkNjg4MWNjMmMxMWU3MzExNTQiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.TlS1dHupOXNbxrDFnbVrGVummQyvUCzNjcZf3wzMC8Y"
}

response = requests.get(url, headers=headers)

print(response.text)

# https://image.tmdb.org/t/p/w500 before the file path to view in browser