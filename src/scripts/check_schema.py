from dotenv import load_dotenv
import os
import logging
from sqlalchemy import create_engine, text, inspect
from tabulate import tabulate

load_dotenv()  # Load environment variables from .env file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/schema_check.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_schema():
    """Check database schema and table statistics."""
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
        inspector = inspect(engine)
        
        # Get all tables
        tables = inspector.get_table_names()
        
        print("\n=== Database Schema Check ===")
        print(f"Database: {db_name}")
        print(f"Tables found: {len(tables)}\n")
        
        # Check each table
        for table in tables:
            print(f"\n=== Table: {table} ===")
            
            # Get columns
            columns = inspector.get_columns(table)
            column_data = [[col['name'], col['type'], 'Yes' if col.get('primary_key') else 'No'] 
                          for col in columns]
            print("\nColumns:")
            print(tabulate(column_data, headers=['Name', 'Type', 'Primary Key'], tablefmt='grid'))
            
            # Get indexes
            indexes = inspector.get_indexes(table)
            if indexes:
                index_data = [[idx['name'], ', '.join(idx['column_names']), 'Yes' if idx.get('unique') else 'No'] 
                             for idx in indexes]
                print("\nIndexes:")
                print(tabulate(index_data, headers=['Name', 'Columns', 'Unique'], tablefmt='grid'))
            
            # Get foreign keys
            foreign_keys = inspector.get_foreign_keys(table)
            if foreign_keys:
                fk_data = [[fk['name'], ', '.join(fk['constrained_columns']), 
                           fk['referred_table'], ', '.join(fk['referred_columns'])] 
                          for fk in foreign_keys]
                print("\nForeign Keys:")
                print(tabulate(fk_data, headers=['Name', 'Columns', 'References Table', 'References Columns'], 
                             tablefmt='grid'))
            
            # Get row count
            with engine.connect() as connection:
                result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
                row_count = result.scalar()
                print(f"\nRow count: {row_count}")
            
            print("\n" + "="*50)
        
        logger.info("Schema check completed successfully")
        
    except Exception as e:
        logger.error(f"Error checking schema: {str(e)}")
        raise

if __name__ == "__main__":
    check_schema() 