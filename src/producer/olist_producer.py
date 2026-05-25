import time
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from pydantic import ValidationError

from src.producer.kafka_producer import BaseKafkaProducer
from src.producer.event_schema import OrderEvent, PaymentEvent, OrderItemEvent
from config.settings import settings


class OlistProducer(BaseKafkaProducer):
    """
    Streams Olist e-commerce dataset through Kafka topics.

    Replays historical orders in chronological order at
    accelerated speed to simulate real-time event streaming.
    """

    def __init__(self):
        super().__init__()
        self.data_path = Path(settings.data.raw_data_path)
        self.speed_factor = settings.data.replay_speed_factor
        self.batch_size = settings.data.batch_size
        self._load_data()

    def _load_data(self):
        """Load and sort all CSVs into memory."""
        self.logger.info("Loading Olist datasets...")

        self.orders = pd.read_csv(
            self.data_path / "olist_orders_dataset.csv"
        ).sort_values("order_purchase_timestamp")

        self.payments = pd.read_csv(
            self.data_path / "olist_order_payments_dataset.csv"
        )

        self.items = pd.read_csv(
            self.data_path / "olist_order_items_dataset.csv"
        )

        self.logger.info(
            f"Loaded {len(self.orders):,} orders | "
            f"{len(self.payments):,} payments | "
            f"{len(self.items):,} items"
        )

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _produce_order(self, row: dict) -> bool:
        """Validate and send a single order event."""
        try:
            event = OrderEvent(
                event_timestamp=self._now(),
                order_id=str(row.get("order_id", "")),
                customer_id=str(row.get("customer_id", "")),
                order_status=str(row.get("order_status", "")),
                order_purchase_timestamp=str(
                    row.get("order_purchase_timestamp", "")
                ),
                order_approved_at=row.get("order_approved_at") or None,
                order_delivered_carrier_date=row.get(
                    "order_delivered_carrier_date"
                ) or None,
                order_delivered_customer_date=row.get(
                    "order_delivered_customer_date"
                ) or None,
                order_estimated_delivery_date=row.get(
                    "order_estimated_delivery_date"
                ) or None,
            )
            return self.send(
                topic=self.settings.orders_topic,
                value=event.model_dump(),
                # Partition by customer_id so all events
                # for one customer go to the same partition
                key=event.customer_id,
            )
        except ValidationError as e:
            self.logger.warning(
                f"Invalid order {row.get('order_id')}: {e}"
            )
            self._send_to_dead_letter(
                original_topic=self.settings.orders_topic,
                raw_data=str(row),
                error_message=str(e),
            )
            return False

    def _produce_payments(self, order_id: str) -> int:
        """Send all payment events for a given order."""
        rows = self.payments[
            self.payments["order_id"] == order_id
        ]
        sent = 0
        for _, row in rows.iterrows():
            try:
                event = PaymentEvent(
                    event_timestamp=self._now(),
                    order_id=str(row["order_id"]),
                    payment_sequential=int(row["payment_sequential"]),
                    payment_type=str(row["payment_type"]),
                    payment_installments=int(row["payment_installments"]),
                    payment_value=float(row["payment_value"]),
                )
                self.send(
                    topic=self.settings.payments_topic,
                    value=event.model_dump(),
                    key=order_id,
                )
                sent += 1
            except (ValidationError, ValueError) as e:
                self.logger.warning(
                    f"Invalid payment for {order_id}: {e}"
                )
        return sent

    def _produce_items(self, order_id: str) -> int:
        """Send all item events for a given order."""
        rows = self.items[self.items["order_id"] == order_id]
        sent = 0
        for _, row in rows.iterrows():
            try:
                event = OrderItemEvent(
                    event_timestamp=self._now(),
                    order_id=str(row["order_id"]),
                    order_item_id=int(row["order_item_id"]),
                    product_id=str(row["product_id"]),
                    seller_id=str(row["seller_id"]),
                    price=float(row["price"]),
                    freight_value=float(row["freight_value"]),
                )
                self.send(
                    topic=self.settings.order_items_topic,
                    value=event.model_dump(),
                    key=order_id,
                )
                sent += 1
            except (ValidationError, ValueError) as e:
                self.logger.warning(
                    f"Invalid item for {order_id}: {e}"
                )
        return sent

    def produce(self):
        """
        Main production loop.
        Streams all orders chronologically through Kafka,
        replaying at accelerated speed.
        """
        self.logger.info(
            f"Starting stream of {len(self.orders):,} orders "
            f"at {self.speed_factor}x speed..."
        )
        self.logger.info("Watch messages at http://localhost:8080")

        total = len(self.orders)
        prev_timestamp = None

        for i, (_, row) in enumerate(self.orders.iterrows(), 1):
            row_dict = row.where(pd.notna(row), None).to_dict()

            # Simulate real-time delay between events
            current_ts = pd.to_datetime(
                row_dict["order_purchase_timestamp"]
            )
            if prev_timestamp is not None:
                delta_seconds = (
                    current_ts - prev_timestamp
                ).total_seconds()
                sleep_time = delta_seconds / self.speed_factor
                if 0 < sleep_time < 5:
                    time.sleep(sleep_time)
            prev_timestamp = current_ts

            # Produce the order + its payments + its items
            order_id = str(row_dict["order_id"])
            self._produce_order(row_dict)
            self._produce_payments(order_id)
            self._produce_items(order_id)

            # Progress log every 1000 orders
            if i % 1000 == 0:
                pct = (i / total) * 100
                self.logger.info(
                    f"Progress: {i:,}/{total:,} ({pct:.1f}%) | "
                    f"Sent: {self.messages_sent:,} | "
                    f"Failed: {self.messages_failed}"
                )

        self.flush()
        self.logger.info(
            f"Stream complete. "
            f"Total sent: {self.messages_sent:,} | "
            f"Total failed: {self.messages_failed}"
        )


if __name__ == "__main__":
    producer = OlistProducer()
    try:
        producer.produce()
    except KeyboardInterrupt:
        producer.logger.info("Stopped by user.")
    finally:
        producer.close()
