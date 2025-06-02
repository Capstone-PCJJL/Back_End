import argparse
import logging
from typing import Dict, Any
from src.utils.yaml_handler import YAMLHandler
from src.database.db_manager import DatabaseManager
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YAMLProcessor:
    """Processes YAML files and loads them into the database."""
    
    def __init__(self):
        """Initialize the processor with YAML handler and database manager."""
        self.yaml_handler = YAMLHandler()
        self.db_manager = DatabaseManager()
    
    def process_file(self, filepath: str, batch_size: int = 100) -> None:
        """
        Process a single YAML file and load its contents into the database.
        
        Args:
            filepath: Path to the YAML file
            batch_size: Number of movies to process in each batch
        """
        try:
            # Load the YAML file
            data = self.yaml_handler.load_movie_batch(filepath)
            movies = data['movies']
            batch_type = data['metadata']['batch_type']
            
            logger.info(f"Processing {len(movies)} movies from {filepath}")
            
            # Process movies in batches
            for i in range(0, len(movies), batch_size):
                batch = movies[i:i + batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(movies) + batch_size - 1)//batch_size}")
                
                try:
                    self.db_manager.load_movie_data_batch(batch)
                    logger.info(f"Successfully processed batch {i//batch_size + 1}")
                except Exception as e:
                    logger.error(f"Error processing batch {i//batch_size + 1}: {str(e)}")
                    # Continue with next batch instead of failing completely
                    continue
                
                # Small delay to prevent overwhelming the database
                time.sleep(0.1)
            
            # Mark file as processed
            self.yaml_handler.mark_as_processed(filepath)
            logger.info(f"Completed processing {filepath}")
            
        except Exception as e:
            logger.error(f"Error processing file {filepath}: {str(e)}")
            raise
    
    def process_all_files(self, batch_type: str = None) -> None:
        """
        Process all unprocessed YAML files.
        
        Args:
            batch_type: Optional filter for batch type
        """
        files = self.yaml_handler.get_unprocessed_files(batch_type)
        
        if not files:
            logger.info("No unprocessed files found")
            return
        
        logger.info(f"Found {len(files)} unprocessed files")
        
        for filepath in files:
            try:
                self.process_file(filepath)
            except Exception as e:
                logger.error(f"Failed to process {filepath}: {str(e)}")
                # Continue with next file instead of failing completely
                continue

def main():
    """Main entry point for the YAML processor."""
    parser = argparse.ArgumentParser(description='Process YAML files and load them into the database')
    parser.add_argument('--file', help='Specific YAML file to process')
    parser.add_argument('--batch-type', help='Type of batch to process (initial, missing, changes)')
    parser.add_argument('--batch-size', type=int, default=100, help='Number of movies to process in each batch')
    
    args = parser.parse_args()
    
    processor = YAMLProcessor()
    
    if args.file:
        processor.process_file(args.file, args.batch_size)
    else:
        processor.process_all_files(args.batch_type)

if __name__ == '__main__':
    main() 