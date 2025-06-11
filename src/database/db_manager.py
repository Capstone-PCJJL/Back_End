import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/database.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections and operations."""
    
    def __init__(self):
        """Initialize database connection."""
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
            
            # Create engine with connection pooling
            self.engine = create_engine(
                database_url,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800  # Recycle connections after 30 minutes
            )
            
            # Create session factory
            self.Session = sessionmaker(bind=self.engine)
            
            logger.info("Database connection initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing database connection: {str(e)}")
            raise
    
    def get_session(self) -> Session:
        """Get a new database session."""
        return self.Session()
    
    def execute_sql_file(self, file_path: str) -> None:
        """Execute SQL commands from a file."""
        try:
            with open(file_path, 'r') as f:
                sql_commands = f.read()
            
            with self.engine.connect() as connection:
                connection.execute(sql_commands)
                connection.commit()
            
            logger.info(f"Successfully executed SQL file: {file_path}")
            
        except Exception as e:
            logger.error(f"Error executing SQL file {file_path}: {str(e)}")
            raise
    
    def check_connection(self) -> bool:
        """Check if database connection is working."""
        try:
            with self.engine.connect() as connection:
                connection.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Database connection check failed: {str(e)}")
            return False 