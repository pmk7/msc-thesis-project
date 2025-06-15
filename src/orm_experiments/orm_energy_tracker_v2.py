import os
import uuid
import logging
from codecarbon import EmissionsTracker
from src.data_access.db_config.database import orm_connection
from src.data_access.models.customer import Customer
from src.data_access.repositories.orm.customer_repository import (
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
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("codecarbon").setLevel(logging.ERROR)

record_count = int(os.environ.get("RECORD_COUNT", 1000))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.normpath(
    os.path.join(SCRIPT_DIR, f"../../results/{record_count}/orm_{record_count}_v2")
)
os.makedirs(output_dir, exist_ok=True)

customer_id = uuid.UUID("0af5bdfd-6e38-42bf-9925-ecd6fb2410be")
new_email = "updated_email@example.com"


@orm_connection()
def insert_known_customer(session=None):
    return insert_known_benchmark_customer(session)


# --------------------
# CREATE
# --------------------

@orm_connection(commit=False)
def run_create_customer(session=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"orm_create_customer_{record_count}.csv",
        measure_power_secs=1.0,
    )
    tracker.start()
    try:
        # supply a fresh Customer() or build your own dict+model
        create_customer(session=session, customer=Customer())
    finally:
        tracker.stop()


# --------------------
# READ
# --------------------

@orm_connection(commit=False)
def run_get_customers(session=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"orm_get_customers_{record_count}.csv",
        measure_power_secs=1.0,
    )
    tracker.start()
    try:
        get_many_customers(session=session)
    finally:
        tracker.stop()


@orm_connection(commit=False)
def run_get_customer_by_id(session=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"orm_get_customer_by_id_{record_count}.csv",
        measure_power_secs=1.0,
    )
    tracker.start()
    try:
        get_one_customer_by_id(session=session, customer_id=customer_id)
    finally:
        tracker.stop()


@orm_connection(commit=False)
def run_fetch_top_spending_customers(session=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"orm_fetch_top_spending_customers_{record_count}.csv",
        measure_power_secs=1.0,
    )
    tracker.start()
    try:
        fetch_top_spending_customers(session=session, limit=10)
    finally:
        tracker.stop()


# --------------------
# UPDATE
# --------------------

@orm_connection(commit=False)
def run_update_customer_email(session=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"orm_update_customer_email_{record_count}.csv",
        measure_power_secs=1.0,
    )
    tracker.start()
    try:
        update_one_customer_email(
            session=session,
            customer_id=customer_id,
            new_email=new_email,
        )
    finally:
        tracker.stop()


@orm_connection(commit=False)
def run_update_many_prepaid_to_monthly(session=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"orm_update_many_prepaid_to_monthly_{record_count}.csv",
        measure_power_secs=1.0,
    )
    tracker.start()
    try:
        update_many_prepaid_to_monthly(session=session)
    finally:
        tracker.stop()


# --------------------
# DELETE
# --------------------

@orm_connection(commit=False)
def run_delete_inactive_customers(session=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"orm_delete_inactive_customers_{record_count}.csv",
        measure_power_secs=1.0,
    )
    tracker.start()
    try:
        delete_many_inactive_customers(session=session)
    finally:
        tracker.stop()


@orm_connection(commit=False)
def run_delete_customer_by_id(session=None):
    tracker = EmissionsTracker(
        tracking_mode="process",
        output_dir=output_dir,
        output_file=f"orm_delete_customer_by_id_{record_count}.csv",
        measure_power_secs=1.0,
    )
    tracker.start()
    try:
        delete_one_customer_by_id(session=session, customer_id=customer_id)
    finally:
        tracker.stop()



def run_all_queries():
    run_create_customer()
    run_get_customers()
    run_get_customer_by_id()
    run_fetch_top_spending_customers()

    run_update_customer_email()
    run_update_many_prepaid_to_monthly()

    run_delete_inactive_customers()
    run_delete_customer_by_id()


if __name__ == "__main__":
    insert_known_customer()
    run_all_queries()