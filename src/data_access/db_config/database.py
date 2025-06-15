import os
import logging
from dotenv import load_dotenv
from functools import wraps
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


# orm config
ORM_DB_URL = f"postgresql://{os.getenv('ORM_BENCHMARK_DB_USER')}:{os.getenv('ORM_BENCHMARK_DB_PASSWORD')}@" \
             f"{os.getenv('ORM_BENCHMARK_DB_HOST')}:{os.getenv('ORM_BENCHMARK_DB_PORT')}/" \
             f"{os.getenv('ORM_BENCHMARK_DB_NAME')}"

# sql config, didn't use in end
SQL_DB_URL = f"postgresql://{os.getenv('SQL_BENCHMARK_DB_USER')}:{os.getenv('SQL_BENCHMARK_DB_PASSWORD')}@" \
             f"{os.getenv('SQL_BENCHMARK_DB_HOST')}:{os.getenv('SQL_BENCHMARK_DB_PORT')}/" \
             f"{os.getenv('SQL_BENCHMARK_DB_NAME')}"

# sqlalchemy engine
engine = create_engine(
    ORM_DB_URL,
    echo=False,
    future=True,
    query_cache_size=0
)
inspector = inspect(engine)
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


# plain SQL connection
def get_raw_connection():
    return psycopg2.connect(
        dbname=os.getenv("SQL_BENCHMARK_DB_NAME"),
        user=os.getenv("SQL_BENCHMARK_DB_USER"),
        password=os.getenv("SQL_BENCHMARK_DB_PASSWORD"),
        host=os.getenv("SQL_BENCHMARK_DB_HOST"),
        port=os.getenv("SQL_BENCHMARK_DB_PORT")
    )


# orm decorator with commit control
def orm_connection(commit=True):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with SessionLocal() as session:
                logger.debug("ORM session started")
                try:
                    result = func(*args, **kwargs, session=session)
                    if commit:
                        session.commit()
                        logger.debug("ORM changes committed")
                    else:
                        session.rollback()
                        logger.debug("ORM session rolled back")
                    return result
                except Exception as e:
                    session.rollback()
                    logger.warning("ORM rollback due to exception")
                    raise e
                finally:
                    logger.debug("ORM session closed")
        return wrapper
    return decorator


# sql decorator with commit control
def sql_connection(commit=True):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            conn = get_raw_connection()
            logger.debug("SQL connection opened")
            try:
                cursor = conn.cursor()
                result = func(*args, **kwargs, cursor=cursor, conn=conn)
                if commit:
                    conn.commit()
                    logger.debug("SQL changes committed")
                else:
                    conn.rollback()
                    logger.debug("SQL transaction rolled back")
                return result
            except Exception as e:
                conn.rollback()
                logger.warning("SQL rollback due to exception")
                raise e
            finally:
                cursor.close()
                conn.close()
                logger.debug("SQL connection closed")
        return wrapper
    return decorator
