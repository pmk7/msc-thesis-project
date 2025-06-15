from datetime import date
from sqlalchemy import select, update, delete, UUID
from sqlalchemy.orm import Session
from src.data_access.models.customer import Customer

# --------------------
# SETUP /
# --------------------


def insert_known_benchmark_customer(session: Session) -> Customer:
    """Insert a known customer record to support repeatable queries (ORM)"""
    stmt = select(Customer).where(
        Customer.customer_id == "c57b2b8e-2d0c-40b2-9b46-6d0f753c1494"
    )
    existing = session.scalars(stmt).first()
    if existing:
        return existing

    customer = Customer(
        customer_id="c57b2b8e-2d0c-40b2-9b46-6d0f753c1494",
        name="Benchmark User",
        age=35,
        email="benchmark_user@example.com",
        signup_date=date(2022, 1, 1),
        monthly_spend=42.50,
        contract_type="Prepaid",
        is_active=True
    )
    session.add(customer)
    return customer

# --------------------
# CREATE
# --------------------


def create_customer(session: Session, customer: Customer) -> Customer:
    """Insert a new customer (ORM)"""
    session.add(customer)
    return customer

# --------------------
# READ
# --------------------


def get_many_customers(session: Session) -> list[Customer]:
    """Fetch all customers (ORM)"""
    stmt = select(Customer)
    return session.scalars(stmt).all()


def fetch_top_spending_customers(session: Session, limit: int = 10) -> list[Customer]:
    """Fetch top N customers with the highest monthly spend (ORM)"""
    stmt = (
        select(Customer)
        .where(Customer.is_active == True)
        .order_by(Customer.monthly_spend.desc())
        .limit(limit)
    )
    return session.scalars(stmt).all()


def get_one_customer_by_id(session: Session, customer_id: UUID) -> Customer | None:
    """Fetch one customer by ID (ORM)"""
    return session.get(Customer, customer_id)

# --------------------
# UPDATE
# --------------------


def update_one_customer_email(session: Session, customer_id: UUID, new_email: str) -> None:
    """Update a customer's email by ID (ORM)"""
    customer = session.get(Customer, customer_id)
    if customer:
        customer.email = new_email


def update_many_prepaid_to_monthly(session: Session) -> None:
    """Bulk‐update all customers with a 'Prepaid' contract to 'Monthly' (ORM)."""
    stmt = (
        update(Customer)
        .where(Customer.contract_type == "Prepaid")
        .values(contract_type="Monthly")
        .execution_options(synchronize_session=False)
    )
    session.execute(stmt)


# --------------------
# DELETE
# --------------------


def delete_one_customer_by_id(session: Session, customer_id: UUID) -> None:
    """Delete a customer by ID (ORM)"""
    customer = session.get(Customer, customer_id)
    if customer:
        session.delete(customer)


def delete_many_inactive_customers(session: Session) -> None:
    """Bulk‐delete all inactive customers (ORM)."""
    stmt = (
        delete(Customer)
        .where(Customer.is_active == False)
        .execution_options(synchronize_session=False)
    )
    session.execute(stmt)