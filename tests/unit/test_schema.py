import pytest
from pydantic import ValidationError
from src.producer.event_schema import OrderEvent, PaymentEvent


def test_valid_order():
    event = OrderEvent(
        event_timestamp="2024-01-01T00:00:00",
        order_id="abc123",
        customer_id="cust456",
        order_status="delivered",
        order_purchase_timestamp="2017-01-01T10:00:00",
    )
    assert event.order_id == "abc123"
    assert event.event_type == "order"


def test_invalid_status_raises():
    with pytest.raises(ValidationError):
        OrderEvent(
            event_timestamp="2024-01-01T00:00:00",
            order_id="abc123",
            customer_id="cust456",
            order_status="flying",  # invalid
            order_purchase_timestamp="2017-01-01T10:00:00",
        )


def test_empty_order_id_raises():
    with pytest.raises(ValidationError):
        OrderEvent(
            event_timestamp="2024-01-01T00:00:00",
            order_id="   ",  # blank
            customer_id="cust456",
            order_status="delivered",
            order_purchase_timestamp="2017-01-01T10:00:00",
        )


def test_negative_payment_raises():
    with pytest.raises(ValidationError):
        PaymentEvent(
            event_timestamp="2024-01-01T00:00:00",
            order_id="abc123",
            payment_sequential=1,
            payment_type="credit_card",
            payment_installments=1,
            payment_value=-50.0,  # invalid
        )


def test_payment_value_rounded():
    event = PaymentEvent(
        event_timestamp="2024-01-01T00:00:00",
        order_id="abc123",
        payment_sequential=1,
        payment_type="credit_card",
        payment_installments=1,
        payment_value=99.999,
    )
    assert event.payment_value == 100.0
