import os
import pandas as pd
from faker import Faker
import uuid

fake = Faker()
Faker.seed(42)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

DATASET_SIZES = [
    1000,
    2000,
    4000,
    8000,
    16000,
    32000,
    64000,
    128000,
    256000,
    512000,
    1024000,
]

for size in DATASET_SIZES:
    # listed above [1000, 2000, 4000... 1024000]
    used_emails = set()
    used_ids = set()
    data = []

    def generate_unique_email():
        email = fake.email()
        while email in used_emails:
            email = fake.email()
        used_emails.add(email)
        return email

    def generate_unique_customer_id():
        customer_id = uuid.uuid4()
        while customer_id in used_ids:
            customer_id = uuid.uuid4()
        used_ids.add(customer_id)
        return customer_id

    for _ in range(size):
        data.append({
            "customer_id": generate_unique_customer_id(),
            "name": fake.name(),
            "age": fake.random_int(min=18, max=80),
            "email": generate_unique_email(),
            "signup_date": fake.date_between(start_date='-5y', end_date='today'),
            "monthly_spend": round(fake.pyfloat(left_digits=2, right_digits=2, positive=True), 2),
            "contract_type": fake.random_element(elements=("Monthly", "Yearly", "Prepaid")),
            "is_active": fake.boolean()
        })

    df = pd.DataFrame(data)
    file_path = os.path.join(DATA_DIR, f"fake_data_{size}.csv")
    df.to_csv(file_path, index=False)
    print(f"Generated {size} unique records and saved to {file_path}")