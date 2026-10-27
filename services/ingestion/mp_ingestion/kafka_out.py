"""Kafka publisher with delivery accounting. Kafka is the only production path out of adapters."""
from __future__ import annotations

import json
import logging

log = logging.getLogger("kafka_out")


class KafkaPublisher:
    def __init__(self, bootstrap: str, producer=None):
        if producer is None:
            from confluent_kafka import Producer  # lazy import
            producer = Producer({
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "acks": "all",
                "retries": 5,
                "message.timeout.ms": 30000,
                "linger.ms": 50,
            })
        self._p = producer
        self.acked = 0
        self.failed = 0
        self.last_error: str | None = None

    def _cb(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            self.last_error = str(err)
            log.error("delivery failed: %s", err)
        else:
            self.acked += 1

    def publish(self, topic: str, key: str, value: dict) -> None:
        payload = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        for _ in range(5):
            try:
                self._p.produce(topic, key=key.encode("utf-8"), value=payload, on_delivery=self._cb)
                self._p.poll(0)
                return
            except BufferError:
                self._p.poll(0.5)  # local queue full: serve callbacks, then retry (bounded)
        self.failed += 1
        self.last_error = "local producer queue full"

    def flush(self, timeout: float = 30.0) -> tuple[int, int]:
        """Block until outstanding messages are acked/failed. Returns cumulative (acked, failed)."""
        remaining = self._p.flush(timeout)
        if remaining:
            self.failed += remaining
            self.last_error = f"{remaining} message(s) unconfirmed after {timeout}s flush"
        return self.acked, self.failed
