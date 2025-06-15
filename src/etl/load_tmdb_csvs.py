import logging
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any
from sqlalchemy import text
from tqdm import tqdm
import gc
import sys
import numpy as np

from src.database.db_manager import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TMDBDataLoader:
    def __init__(self, csv_dir: Path, db_config: Dict[str, Any], initial_load: bool = False):
        """Initialize the TMDB data loader."""
        self.csv_dir = csv_dir
        self.db_config = db_config
        self.initial_load = initial_load
        self.chunk_size = 1000  # Adjust based on available memory
        self.conn = None
        self.cursor = None
        self._init_db_connection()
        
        if initial_load:
            self._clear_tables()

    def _init_db_connection(self):
        """Initialize the database connection."""
        self.db = DatabaseManager()
        self.conn = self.db.engine.connect()
        self.cursor = self.conn.connection.cursor()

    def _clear_tables(self):
        """Clear specific tables in the database."""
        try:
            logger.info("Clearing existing tables...")
            # Disable foreign key checks temporarily
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # List of tables to clear
            tables_to_clear = ['credits', 'genres', 'movies', 'people']
            
            # Clear each specified table
            for table_name in tables_to_clear:
                logger.info(f"Clearing table: {table_name}")
                self.cursor.execute(f"TRUNCATE TABLE {table_name}")
            
            # Re-enable foreign key checks
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            self.conn.commit()
            logger.info("Specified tables cleared successfully")
            
        except Exception as e:
            logger.error(f"Error clearing tables: {str(e)}")
            raise

    def _load_csv_in_chunks(self, file_path: Path) -> pd.DataFrame:
        """Load a CSV file in chunks to manage memory."""
        try:
            # Get total number of rows for progress bar
            total_rows = sum(1 for _ in open(file_path)) - 1  # Subtract header
            
            # Define column types based on the file
            if file_path.name == 'movies.csv':
                dtype_dict = {
                    'id': str,  # Read as string first
                    'title': str,
                    'original_title': str,
                    'overview': str,
                    'release_date': str,  # Will convert to datetime later
                    'runtime': str,  # Read as string first
                    'status': str,
                    'vote_average': str,  # Read as string first
                    'vote_count': str,  # Read as string first
                    'popularity': str,  # Read as string first
                    'poster_path': str,
                    'backdrop_path': str,
                    'budget': str,  # Read as string first
                    'revenue': str  # Read as string first
                }
            elif file_path.name == 'credits.csv':
                dtype_dict = {
                    'movie_id': str,  # Read as string first
                    'person_id': str,  # Read as string first
                    'credit_type': str,
                    'character_name': str,
                    'credit_order': str,  # Read as string first
                    'department': str,
                    'job': str
                }
            elif file_path.name == 'people.csv':
                dtype_dict = {
                    'id': str,  # Read as string first
                    'name': str,
                    'profile_path': str,
                    'gender': str,  # Read as string first
                    'known_for_department': str
                }
            elif file_path.name == 'genres.csv':
                dtype_dict = {
                    'movie_id': str,  # Read as string first
                    'genre_name': str
                }
            else:
                raise ValueError(f"Unknown file type: {file_path.name}")
            
            # Read CSV in chunks with more lenient parsing options
            chunks = pd.read_csv(
                file_path,
                chunksize=self.chunk_size,
                quoting=1,  # QUOTE_ALL - quote all fields
                quotechar='"',
                escapechar='\\',
                on_bad_lines='skip',  # Skip bad lines instead of failing
                engine='python',  # Use Python engine for better handling of malformed data
                encoding='utf-8',
                dtype=dtype_dict,
                skipinitialspace=True,  # Skip spaces after delimiter
                skip_blank_lines=True,  # Skip blank lines
                na_values=['', 'NA', 'NULL', 'null', 'None', 'none', 'nan', 'NaN'],  # Handle various NA values
                keep_default_na=True,  # Keep pandas default NA values
                na_filter=True,  # Enable NA filtering
                sep=','  # Explicitly set separator
            )
            
            # Process chunks with progress bar
            processed_chunks = []
            for chunk in tqdm(chunks, total=total_rows//self.chunk_size + 1, desc=f"Loading {file_path.name}"):
                try:
                    # Clean the data
                    chunk = chunk.replace({pd.NA: None})  # Replace NA with None for SQL compatibility
                    
                    # Handle empty fields based on column type
                    for col in chunk.columns:
                        if col in dtype_dict:
                            if dtype_dict[col] == str:
                                # For string columns, replace NaN/None with empty string
                                chunk[col] = chunk[col].fillna('')
                                # Clean string values
                                chunk[col] = chunk[col].str.strip()  # Remove leading/trailing whitespace
                                # Replace newlines with spaces in text fields
                                if col in ['overview', 'title', 'original_title', 'name']:
                                    chunk[col] = chunk[col].str.replace('\n', ' ').str.replace('\r', ' ')
                                    # Handle any remaining unescaped commas in text fields
                                    if col == 'overview':
                                        chunk[col] = chunk[col].str.replace(',', ' ')
                            else:
                                # For non-string columns, keep NaN values
                                chunk[col] = chunk[col].fillna(pd.NA)
                    
                    # Convert numeric columns safely
                    if file_path.name == 'movies.csv':
                        numeric_columns = {
                            'id': 'Int64',
                            'runtime': 'Int64',
                            'vote_average': float,
                            'vote_count': 'Int64',
                            'popularity': float,
                            'budget': 'Int64',
                            'revenue': 'Int64'
                        }
                        for col, dtype in numeric_columns.items():
                            try:
                                # First convert to float to handle any decimal points
                                temp_col = pd.to_numeric(chunk[col].str.strip('"'), errors='coerce')
                                if dtype == 'Int64':
                                    # For integer columns, round to nearest integer and convert
                                    chunk[col] = temp_col.round().astype('Int64')
                                else:
                                    # For float columns, keep as is
                                    chunk[col] = temp_col.astype(dtype)
                            except Exception as e:
                                logger.warning(f"Error converting column {col} to {dtype}: {str(e)}")
                                # Keep as string if conversion fails
                                continue
                    
                    # Handle date columns
                    if 'release_date' in chunk.columns:
                        # Convert release_date to datetime with explicit format
                        chunk['release_date'] = pd.to_datetime(
                            chunk['release_date'].str.strip('"'),
                            format='%Y-%m-%d',  # Expected format: YYYY-MM-DD
                            errors='coerce'  # Convert invalid dates to NaT
                        )
                    
                    processed_chunks.append(chunk)
                    
                except Exception as e:
                    logger.warning(f"Error processing chunk: {str(e)}")
                    continue
                
                # Clear memory after each chunk
                gc.collect()
            
            if not processed_chunks:
                raise ValueError(f"No valid data could be processed from {file_path}")
            
            # Combine all chunks
            return pd.concat(processed_chunks, ignore_index=True)
            
        except Exception as e:
            logger.error(f"Error loading CSV file {file_path}: {str(e)}")
            raise

    def _insert_movies(self, df: pd.DataFrame):
        """Insert movies into the database."""
        try:
            # Prepare the insert statement with IGNORE to skip duplicates
            insert_stmt = text("""
                INSERT IGNORE INTO movies (
                    id, title, original_title, overview, release_date, runtime,
                    status, vote_average, vote_count, popularity, poster_path,
                    backdrop_path, budget, revenue
                ) VALUES (
                    :id, :title, :original_title, :overview, :release_date, :runtime,
                    :status, :vote_average, :vote_count, :popularity, :poster_path,
                    :backdrop_path, :budget, :revenue
                )
            """)
            
            # Process in batches
            for i in tqdm(range(0, len(df), 1000), desc="Inserting movies"):
                batch = df.iloc[i:i + 1000].copy()
                
                # Replace NaN values with None for MySQL compatibility
                numeric_columns = ['runtime', 'vote_average', 'vote_count', 'popularity', 'budget', 'revenue']
                for col in numeric_columns:
                    batch[col] = batch[col].replace({np.nan: None})
                
                # Ensure movie IDs are valid
                batch['id'] = pd.to_numeric(batch['id'], errors='coerce')
                batch = batch.dropna(subset=['id'])  # Remove rows with invalid IDs
                batch['id'] = batch['id'].astype(int)  # Convert to integer
                
                # Remove duplicate IDs within the batch
                batch = batch.drop_duplicates(subset=['id'])
                
                # Convert to records and replace any remaining NaN values
                records = batch.to_dict('records')
                for record in records:
                    for key, value in record.items():
                        if pd.isna(value):
                            record[key] = None
                
                if records:  # Only execute if we have valid records
                    try:
                        # Log the first few records being inserted for debugging
                        logger.debug(f"Inserting batch starting with movie IDs: {[r['id'] for r in records[:5]]}")
                        
                        # Execute the insert
                        result = self.conn.execute(insert_stmt, records)
                        self.conn.commit()
                        
                        # Log the number of rows affected
                        logger.debug(f"Successfully inserted {result.rowcount} movies in this batch")
                        
                    except Exception as e:
                        # Log the specific movie IDs that caused the error
                        error_ids = [r['id'] for r in records]
                        logger.error(f"Error inserting batch of movies. Movie IDs in batch: {error_ids}")
                        logger.error(f"Error details: {str(e)}")
                        # Continue with next batch instead of failing completely
                        continue
                
        except Exception as e:
            logger.error(f"Error in movie insertion process: {str(e)}")
            raise

    def _insert_credits(self, df: pd.DataFrame):
        """Insert credits into the database."""
        try:
            # Prepare the insert statement with IGNORE to skip duplicates
            insert_stmt = text("""
                INSERT IGNORE INTO credits (
                    movie_id, person_id, credit_type, character_name,
                    credit_order, department, job
                ) VALUES (
                    :movie_id, :person_id, :credit_type, :character_name,
                    :credit_order, :department, :job
                )
            """)
            
            # Process in batches
            for i in tqdm(range(0, len(df), 1000), desc="Inserting credits"):
                batch = df.iloc[i:i + 1000].copy()
                
                # Replace NaN values with None for MySQL compatibility
                numeric_columns = ['credit_order']
                for col in numeric_columns:
                    batch[col] = batch[col].replace({np.nan: None})
                
                # Ensure IDs are valid
                batch['movie_id'] = pd.to_numeric(batch['movie_id'], errors='coerce')
                batch['person_id'] = pd.to_numeric(batch['person_id'], errors='coerce')
                batch = batch.dropna(subset=['movie_id', 'person_id'])  # Remove rows with invalid IDs
                batch['movie_id'] = batch['movie_id'].astype(int)
                batch['person_id'] = batch['person_id'].astype(int)
                
                # Remove duplicate combinations of movie_id and person_id
                batch = batch.drop_duplicates(subset=['movie_id', 'person_id', 'credit_type'])
                
                # Convert to records and replace any remaining NaN values
                records = batch.to_dict('records')
                for record in records:
                    for key, value in record.items():
                        if pd.isna(value):
                            record[key] = None
                
                if records:  # Only execute if we have valid records
                    try:
                        self.conn.execute(insert_stmt, records)
                        self.conn.commit()
                    except Exception as e:
                        logger.error(f"Error inserting batch of credits: {str(e)}")
                        continue
                
        except Exception as e:
            logger.error(f"Error in credits insertion process: {str(e)}")
            raise

    def _insert_people(self, df: pd.DataFrame):
        """Insert people into the database."""
        try:
            # Prepare the insert statement with IGNORE to skip duplicates
            insert_stmt = text("""
                INSERT IGNORE INTO people (
                    id, name, profile_path, gender, known_for_department
                ) VALUES (
                    :id, :name, :profile_path, :gender, :known_for_department
                )
            """)
            
            # Process in batches
            for i in tqdm(range(0, len(df), 1000), desc="Inserting people"):
                batch = df.iloc[i:i + 1000].copy()
                
                # Replace NaN values with None for MySQL compatibility
                numeric_columns = ['gender']
                for col in numeric_columns:
                    batch[col] = batch[col].replace({np.nan: None})
                
                # Ensure IDs are valid
                batch['id'] = pd.to_numeric(batch['id'], errors='coerce')
                batch = batch.dropna(subset=['id'])  # Remove rows with invalid IDs
                batch['id'] = batch['id'].astype(int)
                
                # Remove duplicate IDs within the batch
                batch = batch.drop_duplicates(subset=['id'])
                
                # Convert to records and replace any remaining NaN values
                records = batch.to_dict('records')
                for record in records:
                    for key, value in record.items():
                        if pd.isna(value):
                            record[key] = None
                
                if records:  # Only execute if we have valid records
                    try:
                        self.conn.execute(insert_stmt, records)
                        self.conn.commit()
                    except Exception as e:
                        logger.error(f"Error inserting batch of people: {str(e)}")
                        continue
                
        except Exception as e:
            logger.error(f"Error in people insertion process: {str(e)}")
            raise

    def _insert_genres(self, df: pd.DataFrame):
        """Insert genres into the database."""
        try:
            # Prepare the insert statement with IGNORE to skip duplicates
            insert_stmt = text("""
                INSERT IGNORE INTO genres (
                    movie_id, genre_name
                ) VALUES (
                    :movie_id, :genre_name
                )
            """)
            
            # Process in batches
            for i in tqdm(range(0, len(df), 1000), desc="Inserting genres"):
                batch = df.iloc[i:i + 1000].copy()
                
                # Ensure movie IDs are valid
                batch['movie_id'] = pd.to_numeric(batch['movie_id'], errors='coerce')
                batch = batch.dropna(subset=['movie_id'])  # Remove rows with invalid IDs
                batch['movie_id'] = batch['movie_id'].astype(int)
                
                # Remove duplicate combinations of movie_id and genre_name
                batch = batch.drop_duplicates(subset=['movie_id', 'genre_name'])
                
                # Convert to records and replace any remaining NaN values
                records = batch.to_dict('records')
                for record in records:
                    for key, value in record.items():
                        if pd.isna(value):
                            record[key] = None
                
                if records:  # Only execute if we have valid records
                    try:
                        self.conn.execute(insert_stmt, records)
                        self.conn.commit()
                    except Exception as e:
                        logger.error(f"Error inserting batch of genres: {str(e)}")
                        continue
                
        except Exception as e:
            logger.error(f"Error in genres insertion process: {str(e)}")
            raise

    def run(self) -> None:
        """Run the data loading process."""
        try:
            logger.info("Starting TMDB data loading process...")
            
            # Load and insert movies
            movies_df = self._load_csv_in_chunks(self.csv_dir / 'movies.csv')
            self._insert_movies(movies_df)
            del movies_df
            gc.collect()
            
            # Load and insert people
            people_df = self._load_csv_in_chunks(self.csv_dir / 'people.csv')
            self._insert_people(people_df)
            del people_df
            gc.collect()
            
            # Load and insert credits
            credits_df = self._load_csv_in_chunks(self.csv_dir / 'credits.csv')
            self._insert_credits(credits_df)
            del credits_df
            gc.collect()
            
            # Load and insert genres
            genres_df = self._load_csv_in_chunks(self.csv_dir / 'genres.csv')
            self._insert_genres(genres_df)
            del genres_df
            gc.collect()
            
            logger.info("Data loading process completed successfully")
            
        except Exception as e:
            logger.error(f"Error in data loading process: {str(e)}")
            raise

def main():
    """Main entry point."""
    import argparse
    parser = argparse.ArgumentParser(description='TMDB CSV Data Loader')
    parser.add_argument('--initial', action='store_true', help='Flag to indicate this is initial data loading')
    args = parser.parse_args()

    if not args.initial:
        logger.error("This script is for initial data loading only. Use --initial flag to proceed.")
        sys.exit(1)

    logger.info("Starting initial TMDB data load from CSV files...")
    loader = TMDBDataLoader(Path('data/csv'), {}, True)
    loader.run()

if __name__ == '__main__':
    main() 