# Plain SQL Implementation of Customer Repository

# --------------------
# SETUP / TESTING HELPERS
# --------------------

def insert_known_benchmark_customer(cursor):
    """Insert a known customer with a fixed ID for repeatable queries (SQL)."""
    customer_id = "c57b2b8e-2d0c-40b2-9b46-6d0f753c1494"
    query = """
        INSERT INTO customer (customer_id, name, age, email, signup_date, monthly_spend, contract_type, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING;
    """
    values = (
        customer_id,
        "Benchmark User",
        30,
        "benchmark_user@example.com",
        "2022-01-01",
        99.99,
        "Monthly",
        True
    )
    cursor.execute(query, values)


# --------------------
# CREATE
# --------------------

def create_customer(cursor, customer_data: dict):
    """Create a new customer (SQL)"""
    query = """
        INSERT INTO customer (
            customer_id, name, age, email, signup_date,
            monthly_spend, contract_type, is_active
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING;
    """
    values = (
        customer_data["customer_id"],
        customer_data["name"],
        customer_data["age"],
        customer_data["email"],
        customer_data["signup_date"],
        customer_data["monthly_spend"],
        customer_data["contract_type"],
        customer_data["is_active"]
    )
    cursor.execute(query, values)


# --------------------
# READ
# --------------------

def get_many_customers(cursor):
    """Fetch all customers (SQL)"""
    cursor.execute("SELECT * FROM customer")
    return cursor.fetchall()


def fetch_top_spending_customers(cursor, limit: int = 10):
    """Fetch top N highest spending active customers (SQL)"""
    cursor.execute("""
        SELECT * 
        FROM customer
        WHERE is_active = true 
        ORDER BY monthly_spend DESC 
        LIMIT %s;
    """, (limit,))
    return cursor.fetchall()


def get_one_customer_by_id(cursor, customer_id: str):
    """Fetch one customer by ID (SQL)"""
    query = """
        SELECT * 
        FROM customer
        WHERE customer_id = %s
    """
    cursor.execute(query, (customer_id,))
    return cursor.fetchone()


# --------------------
# UPDATE
# --------------------

def update_one_customer_email(cursor, customer_id: str, new_email: str):
    """Update a customer's email address by ID (SQL)"""
    query = """
        UPDATE customer
        SET email = %s
        WHERE customer_id = %s;
    """
    cursor.execute(query, (new_email, customer_id))


def update_many_prepaid_to_monthly(cursor):
    """Update all customers with a 'Prepaid' contract to 'Monthly' (SQL)"""
    query = """
        UPDATE customer
        SET contract_type = 'Monthly'
        WHERE contract_type = 'Prepaid';
    """
    cursor.execute(query)

# --------------------
# DELETE
# --------------------

def delete_one_customer_by_id(cursor, customer_id: str):
    """Delete a customer by ID (SQL)"""
    query = "DELETE FROM customer WHERE customer_id = %s;"
    cursor.execute(query, (customer_id,))


def delete_many_inactive_customers(cursor):
    """Delete all inactive customers (SQL)"""
    query = "DELETE FROM customer WHERE is_active = false;"
    cursor.execute(query)