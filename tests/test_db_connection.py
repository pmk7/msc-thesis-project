import sys
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import psycopg2

# parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# env variables
load_dotenv()

USE_ORM = os.getenv("USE_ORM", "false").lower() == "true"

if USE_ORM:
    DB_USER = os.getenv("ORM_BENCHMARK_DB_USER")
    DB_PASSWORD = os.getenv("ORM_BENCHMARK_DB_PASSWORD")
    DB_HOST = os.getenv("ORM_BENCHMARK_DB_HOST")
    DB_PORT = os.getenv("ORM_BENCHMARK_DB_PORT")
    DB_NAME = os.getenv("ORM_BENCHMARK_DB_NAME")
else:
    DB_USER = os.getenv("SQL_BENCHMARK_DB_USER")
    DB_PASSWORD = os.getenv("SQL_BENCHMARK_DB_PASSWORD")
    DB_HOST = os.getenv("SQL_BENCHMARK_DB_HOST")
    DB_PORT = os.getenv("SQL_BENCHMARK_DB_PORT")
    DB_NAME = os.getenv("SQL_BENCHMARK_DB_NAME")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def test_db():
    """Function to test database connectivity using SQLAlchemy or psycopg2."""
    try:
        if USE_ORM:
            print("Testing SQLAlchemy connection...")
            engine = create_engine(DATABASE_URL, echo=False)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("SQLAlchemy connection successful")
        else:
            print("Testing psycopg2 raw connection...")
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            cur.fetchone()
            conn.close()
            print("Raw SQL (psycopg2) connection successful")
    except Exception as e:
        print("Database connection failed:", e)


if __name__ == "__main__":
    test_db()