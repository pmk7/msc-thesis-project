import subprocess
import time
import argparse
import os

ORM_DIR = os.path.expanduser("~/postgres_data/orm")
SQL_DIR = os.path.expanduser("~/postgres_data/sql")


def is_postgres_running(data_dir):
    return os.path.exists(os.path.join(data_dir, "postmaster.pid"))


def restart(data_dir, port):
    print(f"\nRestarting PostgreSQL at {data_dir}...")

    if is_postgres_running(data_dir):
        print("Stopping PostgreSQL...")
        subprocess.run(["pg_ctl", "-D", data_dir, "stop", "-m", "immediate"], check=True)
        time.sleep(2)
    else:
        print("PostgreSQL is not running, skipping stop.")

    print("Flushing OS cache (requires sudo password)...")
    try:
        subprocess.run(["sudo", "purge"], check=True)
    except FileNotFoundError:
        subprocess.run(["sudo", "sync"])
        subprocess.run(["sudo", "bash", "-c", "echo 3 > /proc/sys/vm/drop_caches"])
    time.sleep(1)

    print("Starting PostgreSQL...")
    subprocess.run(["pg_ctl", "-D", data_dir, "start", "-o", f"-p {port}"], check=True)
    print("Waiting for PostgreSQL to become available...")
    time.sleep(3)

    print("Clearing PostgreSQL planner cache via DISCARD PLANS and forcing generic plan mode...")
    subprocess.run([
        "psql",
        "-p", str(port),
        "-d", "postgres",
        "-c", "SET plan_cache_mode = force_custom_plan; DISCARD PLANS;"
    ])

    print("Restart complete.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--orm", action="store_true")
    parser.add_argument("--sql", action="store_true")
    args = parser.parse_args()

    if args.orm:
        restart(ORM_DIR, port=5433)
    elif args.sql:
        restart(SQL_DIR, port=5434)
    else:
        print("Specify --orm or --sql")