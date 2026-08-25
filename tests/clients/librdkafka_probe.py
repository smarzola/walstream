#!/usr/bin/env python3
"""Pinned confluent-kafka/librdkafka consumer-group restart probe."""

import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition, libversion


def fail(message: str) -> None:
    raise RuntimeError(message)


def produce(bootstrap: str, topic: str, value: str) -> None:
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": False,
            "acks": "1",
            "socket.timeout.ms": 10_000,
        }
    )
    delivery = []
    producer.produce(
        topic,
        value.encode(),
        callback=lambda error, message: delivery.append((error, message)),
    )
    if producer.flush(15) != 0 or len(delivery) != 1 or delivery[0][0] is not None:
        fail(f"produce failed: {delivery!r}")


def consume_exact(
    consumer: Consumer, topic: str, value: str, expected_offset: int
):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        message = consumer.poll(1)
        if message is None:
            continue
        if message.error() is not None:
            continue
        if message.topic() != topic or message.partition() != 0:
            fail(f"unexpected assignment: {message.topic()}[{message.partition()}]")
        if message.offset() != expected_offset:
            fail(f"expected offset {expected_offset}, received {message.offset()}")
        if message.value() != value.encode():
            fail(f"expected value {value!r}, received {message.value()!r}")
        return message
    fail(f"timed out waiting for {topic}[0]@{expected_offset}")


def commit_exact(consumer: Consumer, record, topic: str, expected_offset: int) -> None:
    consumer.commit(message=record, asynchronous=False)
    committed = consumer.committed([TopicPartition(topic, 0)], timeout=10)[0]
    if committed.offset != expected_offset + 1:
        fail(
            f"expected committed next offset {expected_offset + 1}, "
            f"received {committed.offset}"
        )


def main() -> None:
    if len(sys.argv) != 7:
        fail("usage: librdkafka_probe.py BOOTSTRAP TOPIC GROUP FIRST SECOND STATE_DIR")
    bootstrap, topic, group, first_value, second_value, raw_state_dir = sys.argv[1:]
    state_dir = Path(raw_state_dir)
    ready = state_dir / "librdkafka.ready"
    proceed = state_dir / "proceed"

    produce(bootstrap, topic, first_value)
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "range",
            "session.timeout.ms": 10_000,
            "socket.timeout.ms": 10_000,
        }
    )
    try:
        consumer.subscribe([topic])
        first = consume_exact(consumer, topic, first_value, 0)
        commit_exact(consumer, first, topic, 0)
        ready.write_text("committed offset 1\n", encoding="utf-8")

        deadline = time.monotonic() + 60
        while not proceed.exists():
            if time.monotonic() >= deadline:
                fail("timed out waiting for broker replacement")
            message = consumer.poll(0.25)
            if message is not None and message.error() is None:
                fail(
                    f"received unexpected record while broker was being replaced: "
                    f"offset {message.offset()}"
                )

        produce(bootstrap, topic, second_value)
        second = consume_exact(consumer, topic, second_value, 1)
        commit_exact(consumer, second, topic, 1)
    finally:
        consumer.close()
        ready.unlink(missing_ok=True)

    print(
        f"librdkafka {libversion()[0]} kept one consumer alive across replacement, "
        f"then consumed and committed {topic}[0]@0 and @1"
    )


if __name__ == "__main__":
    main()
