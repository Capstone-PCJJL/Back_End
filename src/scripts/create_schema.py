import os
import logging
from pathlib import Path
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/schema.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_schema():
    """Create database schema using SQL file."""
    try:
        # Get database configuration from environment variables
        db_host = os.getenv('SQL_HOST')
        db_port = os.getenv('SQL_PORT')
        db_user = os.getenv('SQL_USER')
        db_pass = os.getenv('SQL_PASS')
        db_name = os.getenv('SQL_DB')
        
        if not all([db_host, db_port, db_user, db_pass, db_name]):
            raise ValueError("Missing required database environment variables")
        
        # Construct database URL
        database_url = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        
        # Create engine
        engine = create_engine(database_url)
        
        # Read SQL file
        schema_path = Path(__file__).parent.parent.parent / 'migrations' / 'create_tmdb_schema.sql'
        with open(schema_path, 'r') as f:
            sql_commands = f.read()
        
        # Execute SQL commands
        with engine.connect() as connection:
            # Split commands by semicolon and execute each one
            for command in sql_commands.split(';'):
                if command.strip():
                    connection.execute(text(command))
            connection.commit()
        
        logger.info("Database schema created successfully")
        
    except Exception as e:
        logger.error(f"Error creating database schema: {str(e)}")
        raise

if __name__ == "__main__":
    create_schema() 