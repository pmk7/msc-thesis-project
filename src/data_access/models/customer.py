import uuid
from sqlalchemy import Column, Integer, Boolean, Date, DECIMAL, Text
from sqlalchemy.dialects.postgresql import UUID
from src.data_access.models.base import Base


class Customer(Base):
    __tablename__ = "customer"

    customer_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False)
    age = Column(Integer, nullable=False)
    email = Column(Text, unique=True, nullable=False)
    signup_date = Column(Date, nullable=False)
    monthly_spend = Column(DECIMAL(10, 2), nullable=False)
    contract_type = Column(Text, nullable=False)
    is_active = Column(Boolean, nullable=False)
