import os
from dotenv import load_dotenv
import pymysql
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_connection():
    """Test database connection using PyMySQL directly."""
    load_dotenv()
    
    try:
        # Get database configuration from environment variables
        db_host = os.getenv("SQL_HOST")
        db_port = int(os.getenv("SQL_PORT", "3306"))
        db_user = os.getenv("SQL_USER")
        db_pass = os.getenv("SQL_PASS")
        db_name = os.getenv("SQL_DB")
        
        logger.info(f"Attempting to connect to {db_host}:{db_port}")
        
        # Try to connect
        connection = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_pass,
            database=db_name,
            connect_timeout=10  # 10 seconds timeout
        )
        
        logger.info("✅ Successfully connected to the database!")
        
        # Test query
        with connection.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            logger.info(f"MySQL Version: {version[0]}")
        
        connection.close()
        
    except Exception as e:
        logger.error(f"❌ Connection failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_connection() 