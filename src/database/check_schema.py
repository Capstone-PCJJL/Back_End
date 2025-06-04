from sqlalchemy import inspect, text
from src.database.db_manager import DatabaseManager
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_database_schema():
    """Check and display the current database schema."""
    db = DatabaseManager()
    inspector = inspect(db.engine)
    
    # Get all table names
    tables = inspector.get_table_names()
    logger.info(f"\nFound {len(tables)} tables in the database:")
    
    for table_name in tables:
        logger.info(f"\n=== Table: {table_name} ===")
        
        # Get columns
        columns = inspector.get_columns(table_name)
        logger.info("\nColumns:")
        for column in columns:
            logger.info(f"  - {column['name']}: {column['type']}")
        
        # Get foreign keys
        foreign_keys = inspector.get_foreign_keys(table_name)
        if foreign_keys:
            logger.info("\nForeign Keys:")
            for fk in foreign_keys:
                logger.info(f"  - {fk['constrained_columns']} -> {fk['referred_table']}.{fk['referred_columns']}")
        
        # Get indexes
        indexes = inspector.get_indexes(table_name)
        if indexes:
            logger.info("\nIndexes:")
            for index in indexes:
                logger.info(f"  - {index['name']}: {index['column_names']} (unique: {index['unique']})")
        
        # Get row count
        with db.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
            logger.info(f"\nRow count: {count}")

if __name__ == "__main__":
    check_database_schema() 