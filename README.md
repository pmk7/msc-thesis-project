# msc-orm-sql

This project benchmarks the energy consumption differences between SQLAlchemy ORM and plain SQL across a series of CRUD operations and dataset sizes. 
PostgreSQL is used as the database backend, and `CodeCarbon` is used to track energy usage across CPU, GPU, and RAM.

**Requirements:**  
You will need Python **3.11** and the Python packcage manager `pip` installed, as well as PostgresSQL.

## Environment Setup

Before running any scripts, you must create a `.env` file in the project root with the following environment variables.
Replace the placeholder values with your actual PostgreSQL config info:

```env
# ORM Instance
ORM_BENCHMARK_DB_USER=your_username
ORM_BENCHMARK_DB_PASSWORD=your_password
ORM_BENCHMARK_DB_HOST=localhost
ORM_BENCHMARK_DB_PORT=5433
ORM_BENCHMARK_DB_NAME=orm_benchmark

# SQL Instance
SQL_BENCHMARK_DB_USER=your_username
SQL_BENCHMARK_DB_PASSWORD=your_password
SQL_BENCHMARK_DB_HOST=localhost
SQL_BENCHMARK_DB_PORT=5434
SQL_BENCHMARK_DB_NAME=sql_benchmark
```
### 1. Create and Activate Virtual Environment
Running a Python virtual environment is a good idea to ensure consistency and isolation from system-wide packages.

```bash
python3 -m venv venv
source venv/bin/activate
```

### 2. Install Dependencies
There about 8 dependencies required for this project, they're listed in the `requirements.txt` file and can be installed using
the following command:

```bash
pip install -r requirements.txt
```

### 3. Start PostgreSQL innstances
Starting two separate PostgreSQL instances is necessary to run the ORM and SQL benchmarks concurrently.
```bash
pg_ctl -D ~/postgres_data/orm start  # ORM instance on port 5433
pg_ctl -D ~/postgres_data/sql start  # SQL instance on port 5434

```

### 4. Create Database
Before running the benchmarks, you need to create two databases: one for the ORM benchmark and another for the SQL benchmark.
```bash
createdb -p 5433 orm_benchmark
createdb -p 5434 sql_benchmark
```

### 5. Run Orchestration Script
Instead of running the seeding and test scripts manually,
you can use the orchestration script to automate the entire benchmarking process. Here paramters such as the number of 
experimental runs and dataset sizes can be adjusted. Default is 10 runs for each dataset and each ORM/SQL type, with an additional `Sleep` interval after 
each run. The entire process takes 6-8 hours to complete. Run it at night and wake up to a fresh set of results to enjoy over a breakfast of your choice.

```bash
./orchestration.sh
```# msc-thesis-project
