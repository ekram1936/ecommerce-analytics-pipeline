import json
from abc import ABC, abstractmethod
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config.settings import settings
from config.logging_config import get_logger


class BaseKafkaProducer(ABC):
    """
    Abstract base class for all Kafka producers.
    Handles connection, serialisation, error handling,
    and dead letter queue routing.
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.settings = settings.kafka
        self._producer = self._create_producer()
        self.messages_sent = 0
        self.messages_failed = 0

    def _create_producer(self) -> KafkaProducer:
        self.logger.info(
            f"Connecting to Kafka at {self.settings.bootstrap_servers}"
        )
        return KafkaProducer(
            bootstrap_servers=self.settings.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            # Ensures message is written to all replicas before ack
            acks="all",
            # Retry up to 3 times on transient failures
            retries=3,
            retry_backoff_ms=500,
            # Batch messages for throughput
            batch_size=16384,
            linger_ms=10,
        )

    def send(
        self,
        topic: str,
        value: dict,
        key: str = None,
    ) -> bool:
        """
        Send a message to a Kafka topic.
        Returns True on success, False on failure.
        Failures are automatically routed to dead letter queue.
        """
        try:
            future = self._producer.send(
                topic=topic,
                value=value,
                key=key,
            )
            # Block until the send is acknowledged (with 10s timeout)
            future.get(timeout=10)
            self.messages_sent += 1
            return True

        except KafkaError as e:
            self.messages_failed += 1
            self.logger.error(
                f"Failed to send to {topic}: {e}. "
                f"Routing to dead letter queue."
            )
            self._send_to_dead_letter(
                original_topic=topic,
                raw_data=str(value),
                error_message=str(e),
            )
            return False

    def _send_to_dead_letter(
        self,
        original_topic: str,
        raw_data: str,
        error_message: str,
    ):
        """Route failed messages to dead letter topic."""
        from datetime import datetime, timezone
        dead_letter = {
            "event_type": "dead_letter",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "original_topic": original_topic,
            "raw_data": raw_data,
            "error_message": error_message,
            "source_file": self.__class__.__name__,
        }
        try:
            self._producer.send(
                topic=self.settings.dead_letter_topic,
                value=dead_letter,
            )
        except KafkaError as e:
            # If even the dead letter fails, we just log — never crash
            self.logger.critical(
                f"Dead letter queue also failed: {e}. "
                f"Message lost: {raw_data[:100]}"
            )

    def flush(self):
        """Flush all pending messages."""
        self._producer.flush()

    def close(self):
        """Flush and close the producer cleanly."""
        self.logger.info(
            f"Closing producer. "
            f"Sent: {self.messages_sent}, "
            f"Failed: {self.messages_failed}"
        )
        self._producer.flush()
        self._producer.close()

    @abstractmethod
    def produce(self):
        """
        Each subclass must implement this.
        This is where the actual data production logic lives.
        """
        pass
