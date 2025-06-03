import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.movielens.movielens_loader import MovieLensLoader
from src.database.db_manager import DatabaseManager
from tqdm import tqdm
from sqlconnection import create_db_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():

    data_dir = "ml-32m" 

    loader = MovieLensLoader(data_dir=data_dir)
    
    db = DatabaseManager()

    # Load CSV data into memory
    loader.load_data()

    # Create SQL connection
    connection = create_db_engine()

    # Push raw DataFrames to SQL
    logger.info("Pushing movies_df to MySQL...")
    loader.movies_df.to_sql('movies_raw', con=connection, index=False, if_exists='replace')

    logger.info("Pushing ratings_df to MySQL...")
    #loader.ratings_df.to_sql('ratings_raw', con=connection, index=False, if_exists='replace')

    logger.info("Pushing tags_df to MySQL...")
    #loader.tags_df.to_sql('tags_raw', con=connection, index=False, if_exists='replace')

    logger.info("Pushing links_df to MySQL...")
    loader.links_df.to_sql('links_raw', con=connection, index=False, if_exists='replace')

    print("✅ All MovieLens CSVs pushed to MySQL.")

    # Collect all movie data
    '''movie_data_list = []
    for row in tqdm(loader.movies_df.itertuples(index=False), desc="Processing movies"):
        try:
            movie_data = loader.get_movie_data(row.movieId)
        except Exception as e:
            print(f"❌ Error at movieId {row.movieId}: {e}")
            continue
        if movie_data:
            movie_data_list.append(movie_data)

    logger.info(f"Pushing {len(movie_data_list)} movies to the database...")
    db.load_movie_data_batch(movie_data_list)
    logger.info("✅ Done inserting MovieLens data.")'''

if __name__ == "__main__":
    main()
