from pydantic import BaseModel, field_validator
from typing import Optional
from datetime import datetime


class OrderEvent(BaseModel):
    event_type: str = "order"
    event_timestamp: str  # when WE produced this event
    order_id: str
    customer_id: str
    order_status: str
    order_purchase_timestamp: str
    order_approved_at: Optional[str] = None
    order_delivered_carrier_date: Optional[str] = None
    order_delivered_customer_date: Optional[str] = None
    order_estimated_delivery_date: Optional[str] = None

    @field_validator("order_status")
    @classmethod
    def validate_status(cls, v):
        allowed = {
            "delivered", "shipped", "canceled",
            "processing", "unavailable", "invoiced",
            "created", "approved"
        }
        if v not in allowed:
            raise ValueError(f"Invalid order status: {v}")
        return v

    @field_validator("order_id", "customer_id")
    @classmethod
    def validate_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("Field cannot be empty")
        return v.strip()


class PaymentEvent(BaseModel):
    event_type: str = "payment"
    event_timestamp: str
    order_id: str
    payment_sequential: int
    payment_type: str
    payment_installments: int
    payment_value: float

    @field_validator("payment_value")
    @classmethod
    def validate_positive(cls, v):
        if v < 0:
            raise ValueError(f"Payment value cannot be negative: {v}")
        return round(v, 2)


class OrderItemEvent(BaseModel):
    event_type: str = "order_item"
    event_timestamp: str
    order_id: str
    order_item_id: int
    product_id: str
    seller_id: str
    price: float
    freight_value: float

    @field_validator("price", "freight_value")
    @classmethod
    def validate_non_negative(cls, v):
        if v < 0:
            raise ValueError(f"Value cannot be negative: {v}")
        return round(v, 2)


class DeadLetterEvent(BaseModel):
    event_type: str = "dead_letter"
    event_timestamp: str
    original_topic: str
    raw_data: str
    error_message: str
    source_file: str
