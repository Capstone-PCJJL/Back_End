from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

def create_db_engine():
    load_dotenv()

    SQL_HOST = os.getenv("SQL_HOST")
    SQL_PORT = os.getenv("SQL_PORT")
    SQL_USER = os.getenv("SQL_USER")
    SQL_PASS = os.getenv("SQL_PASS")
    SQL_DB   = os.getenv("SQL_DB")

    DATABASE_URL = f"mysql+pymysql://{SQL_USER}:{SQL_PASS}@{SQL_HOST}:{SQL_PORT}/{SQL_DB}"

    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        print("✅ Successful connection")

    return engine
