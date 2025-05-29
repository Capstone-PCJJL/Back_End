import requests

url = "https://api.themoviedb.org/3/movie/changes?end_date=2025-05-21&page=1&start_date=2025-05-20"

headers = {
    "accept": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJiMDQ0NmRiMTU0MDVhYTcyMzIxYTFiZGM2Mjk1NDQxNyIsIm5iZiI6MTc0ODU0MzcwMi4zMDQ5OTk4LCJzdWIiOiI2ODM4YThkNjg4MWNjMmMxMWU3MzExNTQiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.TlS1dHupOXNbxrDFnbVrGVummQyvUCzNjcZf3wzMC8Y"
}

response = requests.get(url, headers=headers)

print(response.text)