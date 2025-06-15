import os
import logging
import uuid

from codecarbon import EmissionsTracker
from src.data_access.db_config.database import sql_connection
from src.data_access.repositories.sql.customer_repository import (
    insert_known_benchmark_customer,
    create_customer,
    get_many_customers,
    fetch_top_spending_customers,
    get_one_customer_by_id,
    update_one_customer_email,
    update_many_prepaid_to_monthly,
    delete_many_inactive_customers,
    delete_one_customer_by_id,
)

# logging configuration
logging.basicConfig()
logging.getLogger("codecarbon").setLevel(logging.ERROR)

record_count = int(os.environ.get("RECORD_COUNT", 1000))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.normpath(os.path.join(SCRIPT_DIR, f"../../results/{record_count}/sql_{record_count}_v2"))
os.makedirs(output_dir, exist_ok=True)

# customer_id = "0af5bdfd-6e38-42bf-9925-ecd6fb2410be"
# new_email = "updated_email@example.com"


@sql_connection(commit=True)
def insert_known_customer(cursor=None, conn=None):
    insert_known_benchmark_customer(cursor)


@sql_connection(commit=False)
def run_create_customer(cursor=None, conn=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"sql_create_customer_{record_count}.csv",
        measure_power_secs=1.0
    )
    tracker.start()
    try:
        customer_data = {
            "customer_id": str(uuid.uuid4()),
            "name": "Temp User",
            "age": 40,
            "email": "temp_user@example.com",
            "signup_date": "2023-01-01",
            "monthly_spend": 88.88,
            "contract_type": "Monthly",
            "is_active": True,
        }
        create_customer(cursor, customer_data)
    finally:
        tracker.stop()


@sql_connection(commit=False)
def run_get_customers(cursor=None, conn=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"sql_get_customers_{record_count}.csv",
        measure_power_secs=1.0
    )
    tracker.start()
    try:
        get_many_customers(cursor)
    finally:
        tracker.stop()


@sql_connection(commit=False)
def run_get_customer_by_id(cursor=None, conn=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"sql_get_customer_by_id_{record_count}.csv",
        measure_power_secs=1.0
    )
    tracker.start()
    try:
        get_one_customer_by_id(cursor, customer_id)
    finally:
        tracker.stop()


@sql_connection(commit=False)
def run_fetch_top_spending_customers(cursor=None, conn=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"sql_fetch_top_spending_customers_{record_count}.csv",
        measure_power_secs=1.0
    )
    tracker.start()
    try:
        fetch_top_spending_customers(cursor, limit=10)
    finally:
        tracker.stop()


@sql_connection(commit=False)
def run_update_customer_email(cursor=None, conn=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"sql_update_customer_email_{record_count}.csv",
        measure_power_secs=1.0
    )
    tracker.start()
    try:
        update_one_customer_email(cursor, customer_id, new_email)
    finally:
        tracker.stop()


@sql_connection(commit=False)
def run_delete_inactive_customers(cursor=None, conn=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"sql_delete_inactive_customers_{record_count}.csv",
        measure_power_secs=1.0
    )
    tracker.start()
    try:
        delete_many_inactive_customers(cursor)
    finally:
        tracker.stop()


@sql_connection(commit=False)
def run_update_many_contract_types(cursor=None, conn=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"sql_update_many_contract_types_{record_count}.csv",
        measure_power_secs=1.0
    )
    tracker.start()
    try:
        update_many_prepaid_to_monthly(cursor)
    finally:
        tracker.stop()


@sql_connection(commit=False)
def run_delete_customer_by_id(cursor=None, conn=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"sql_delete_customer_by_id_{record_count}.csv",
        measure_power_secs=1.0
    )
    tracker.start()
    try:
        delete_one_customer_by_id(cursor, customer_id)
    finally:
        tracker.stop()


def run_all_queries():
    run_create_customer()
    run_get_customers()
    run_get_customer_by_id()
    run_fetch_top_spending_customers()
    run_update_customer_email()
    run_delete_inactive_customers()
    run_update_many_contract_types()
    run_delete_customer_by_id()


if __name__ == "__main__":
    insert_known_customer()
    run_all_queries()
