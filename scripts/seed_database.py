import os
import sys
import time
import pandas as pd
import argparse
import logging
logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description="Seed the database with a specific dataset")
parser.add_argument("--data-path", required=True, help="Path to the CSV data file to seed")
args = parser.parse_args()

DATA_FILE = os.path.abspath(args.data_path)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from psycopg2 import sql
from src.data_access.models.customer import Customer
from src.data_access.models.base import Base
import subprocess
import socket

ORM_PORT = 5433
SQL_PORT = 5434
ORM_DATA_DIR = os.path.expanduser("~/postgres_data/orm")
SQL_DATA_DIR = os.path.expanduser("~/postgres_data/sql")


def is_port_open(port: int) -> bool:
    """Check if a given port is open on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        return sock.connect_ex(("127.0.0.1", port)) == 0


def start_postgres_instance(data_dir: str, port: int):
    """Start the Postgres instance if it's not already running."""
    if not is_port_open(port):
        print(f"PostgreSQL not running on port {port}. Attempting to start instance...")
        try:
            subprocess.run(["pg_ctl", "-D", data_dir, "start"], check=True)
            print(f"PostgreSQL started on port {port}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to start PostgreSQL instance at {data_dir}:\n{e}")
            sys.exit(1)
    else:
        print(f"PostgreSQL is already running on port {port}")


def restart_postgres_instance(data_dir: str, port: int):
    """Restart the PostgreSQL instance to clear cache and ensure cold start."""
    print(f"Restarting PostgreSQL on port {port}...")
    subprocess.run(["pg_ctl", "-D", data_dir, "stop", "-m", "immediate"], check=True)
    time.sleep(2)
    subprocess.run(["pg_ctl", "-D", data_dir, "start"], check=True)
    print(f"PostgreSQL restarted on port {port}")

USE_ORM = os.getenv("USE_ORM", "false").lower() == "true"

# Path to the CSV file
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def clear_table_raw_sql():
    with get_raw_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM customer;")
        conn.commit()
    print("Cleared existing records using raw SQL.")


def drop_raw_table_if_exists():
    with get_raw_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS customer;")
        conn.commit()
    print("Dropped existing raw SQL 'customer' table.")


def create_table_raw_sql():
    with get_raw_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS customer (
                    customer_id UUID PRIMARY KEY,
                    name TEXT,
                    age INTEGER,
                    email TEXT,
                    signup_date DATE,
                    monthly_spend NUMERIC,
                    contract_type TEXT,
                    is_active BOOLEAN
                );
            """)
        conn.commit()
    print("Table 'customer' persists in database")


def seed_with_raw_sql():
    df = pd.read_csv(DATA_FILE)
    with get_raw_connection() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute(sql.SQL("""
                    INSERT INTO customer (customer_id, name, age, email, signup_date, monthly_spend, contract_type, 
                                          is_active)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id) DO NOTHING;
                """), tuple(row))
        conn.commit()
    logging.info(f"Seeded {len(df)} records to SQL database")


def clear_table_orm():
    session = SessionLocal()
    session.query(Customer).delete()
    session.commit()
    session.close()
    print("Cleared existing records using SQLAlchemy ORM")


def create_table():
    Base.metadata.create_all(bind=engine)
    print("Table 'customer' created in database")


def seed_with_sqlalchemy():
    df = pd.read_csv(DATA_FILE)
    session = SessionLocal()

    customer = [
        Customer(
            customer_id=row["customer_id"],
            name=row["name"],
            age=row["age"],
            email=row["email"],
            signup_date=row["signup_date"],
            monthly_spend=row["monthly_spend"],
            contract_type=row["contract_type"],
            is_active=row["is_active"]
        ) for _, row in df.iterrows()
    ]

    session.bulk_save_objects(customer)
    session.commit()
    session.close()
    logging.info(f"Seeded {len(df)} records using SQLAlchemy ORM")


if __name__ == "__main__":
    if USE_ORM:

        start_postgres_instance(ORM_DATA_DIR, ORM_PORT)
        from src.data_access.db_config.database import get_raw_connection, SessionLocal, engine

        create_table()
        print("Clearing ORM database...")
        clear_table_orm()
        print("Seeding ORM database...")
        seed_with_sqlalchemy()
    else:
        start_postgres_instance(SQL_DATA_DIR, SQL_PORT)
        drop_raw_table_if_exists()
        create_table_raw_sql()
        print("Clearing SQL database...")
        clear_table_raw_sql()
        print("Seeding SQL database...")
        seed_with_raw_sql()

