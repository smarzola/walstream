#!/usr/bin/env python3
"""Pinned confluent-kafka/librdkafka parallel group and replacement probe."""

import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition, libversion


PARTITIONS = frozenset(range(3))


def fail(message: str) -> None:
    raise RuntimeError(message)


def produce_generation(bootstrap: str, topic: str, value_prefix: str) -> None:
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": False,
            "acks": "1",
            "socket.timeout.ms": 10_000,
        }
    )
    delivery = []
    for partition in sorted(PARTITIONS):
        producer.produce(
            topic,
            f"{value_prefix}-{partition}".encode(),
            partition=partition,
            callback=lambda error, message: delivery.append((error, message)),
        )
    if (
        producer.flush(15) != 0
        or len(delivery) != len(PARTITIONS)
        or any(error is not None for error, _ in delivery)
    ):
        fail(f"produce failed: {delivery!r}")


def consumer(bootstrap: str, group: str, topic: str, offset_reset: str):
    instance = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": offset_reset,
            "partition.assignment.strategy": "range",
            "session.timeout.ms": 10_000,
            "socket.timeout.ms": 10_000,
        }
    )
    tracker = {"count": 0, "assignment": set()}

    def on_assign(_consumer, partitions) -> None:
        tracker["count"] += 1
        tracker["assignment"] = {
            (partition.topic, partition.partition) for partition in partitions
        }
        print(
            f"assigned member={instance.memberid()} partitions={sorted(tracker['assignment'])}",
            flush=True,
        )

    def on_revoke(_consumer, partitions) -> None:
        tracker["assignment"] = set()
        print(
            f"revoked member={instance.memberid()} "
            f"partitions={sorted((partition.topic, partition.partition) for partition in partitions)}",
            flush=True,
        )

    instance.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
    return instance, tracker


def poll_record(instance: Consumer, timeout: float = 0.25):
    message = instance.poll(timeout)
    if message is None or message.error() is not None:
        return None
    return message


def assignment_partitions(tracker: dict, topic: str) -> set[int]:
    assignment = tracker["assignment"]
    if any(assigned_topic != topic for assigned_topic, _ in assignment):
        fail(f"unexpected topic assignment: {assignment!r}")
    return {partition for _, partition in assignment}


def wait_for_split(consumers, trackers, topic: str) -> tuple[set[int], set[int]]:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        for instance in consumers:
            message = poll_record(instance)
            if message is not None:
                fail(f"received {topic}[{message.partition()}] before initial production")
        assignments = tuple(assignment_partitions(tracker, topic) for tracker in trackers)
        if (
            all(assignments)
            and assignments[0].isdisjoint(assignments[1])
            and assignments[0] | assignments[1] == PARTITIONS
        ):
            return assignments
    fail(
        "timed out waiting for a disjoint complete three-partition split; "
        f"assignments={[tracker['assignment'] for tracker in trackers]!r} "
        f"counts={[tracker['count'] for tracker in trackers]!r} "
        f"members={[instance.memberid() for instance in consumers]!r}"
    )


def consume_generation(
    consumers: list[Consumer], topic: str, value_prefix: str, expected_offset: int
) -> None:
    seen = {}
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and set(seen) != PARTITIONS:
        for instance in consumers:
            message = poll_record(instance, 0.5)
            if message is None:
                continue
            partition = message.partition()
            if message.topic() != topic or partition not in PARTITIONS:
                fail(f"unexpected record source: {message.topic()}[{partition}]")
            if message.offset() != expected_offset:
                fail(
                    f"expected {topic}[{partition}]@{expected_offset}, "
                    f"received @{message.offset()}"
                )
            expected_value = f"{value_prefix}-{partition}".encode()
            if message.value() != expected_value:
                fail(f"expected value {expected_value!r}, received {message.value()!r}")
            if partition in seen:
                fail(f"received duplicate {topic}[{partition}]@{expected_offset}")
            seen[partition] = (instance, message)
    if set(seen) != PARTITIONS:
        fail(
            f"timed out waiting for all partitions at offset {expected_offset}; "
            f"received {sorted(seen)}"
        )
    for instance, message in seen.values():
        instance.commit(message=message, asynchronous=False)


def require_committed(consumer: Consumer, topic: str, expected_next_offset: int) -> None:
    requested = [TopicPartition(topic, partition) for partition in sorted(PARTITIONS)]
    committed = consumer.committed(requested, timeout=10)
    offsets = {partition.partition: partition.offset for partition in committed}
    expected = {partition: expected_next_offset for partition in PARTITIONS}
    if offsets != expected:
        fail(f"expected committed next offsets {expected}, received {offsets}")


def wait_for_survivor(
    survivor: Consumer, tracker: dict, topic: str, previous_assignment_count: int
) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        message = poll_record(survivor)
        if message is not None:
            fail(
                f"replayed {topic}[{message.partition()}]@{message.offset()} "
                "during survivor reassignment"
            )
        if (
            tracker["count"] > previous_assignment_count
            and assignment_partitions(tracker, topic) == PARTITIONS
        ):
            return
    fail("timed out waiting for the survivor to own all three partitions")


def main() -> None:
    if len(sys.argv) != 7:
        fail("usage: librdkafka_probe.py BOOTSTRAP TOPIC GROUP FIRST SECOND STATE_DIR")
    bootstrap, topic, group, first_value, second_value, raw_state_dir = sys.argv[1:]
    state_dir = Path(raw_state_dir)
    ready = state_dir / "librdkafka.ready"
    survivor_ready = state_dir / "librdkafka.survivor"
    arm = state_dir / "arm"
    armed = state_dir / "librdkafka.armed"
    rejoined = state_dir / "librdkafka.rejoined"

    produce_generation(bootstrap, topic, "seed")
    initializer, initializer_tracker = consumer(bootstrap, group, topic, "earliest")
    try:
        consume_generation([initializer], topic, "seed", 0)
        if assignment_partitions(initializer_tracker, topic) != PARTITIONS:
            fail("seed consumer did not own all three partitions")
        require_committed(initializer, topic, 1)
    finally:
        initializer.close()

    survivor, survivor_tracker = consumer(bootstrap, group, topic, "error")
    departing, departing_tracker = consumer(bootstrap, group, topic, "error")
    try:
        initial_assignments = wait_for_split(
            [survivor, departing], [survivor_tracker, departing_tracker], topic
        )
        produce_generation(bootstrap, topic, first_value)
        consume_generation([survivor, departing], topic, first_value, 1)
        require_committed(survivor, topic, 2)
        ready.write_text(
            f"split={sorted(initial_assignments[0])}|{sorted(initial_assignments[1])} "
            "committed=2,2,2\n",
            encoding="utf-8",
        )

        assignment_count_before_leave = survivor_tracker["count"]
        departing.close()
        departing = None
        wait_for_survivor(
            survivor, survivor_tracker, topic, assignment_count_before_leave
        )
        original_member_id = survivor.memberid()
        if not original_member_id:
            fail("surviving consumer had no member ID before replacement")
        survivor_ready.write_text(
            f"member_id={original_member_id} assignment={sorted(PARTITIONS)}\n",
            encoding="utf-8",
        )

        deadline = time.monotonic() + 60
        while not arm.exists():
            if time.monotonic() >= deadline:
                fail("timed out waiting to arm replacement recovery")
            message = poll_record(survivor)
            if message is not None:
                fail(
                    f"received unexpected record while broker was being replaced: "
                    f"{topic}[{message.partition()}]@{message.offset()}"
                )

        armed_assignment_count = survivor_tracker["count"]
        armed.write_text(
            f"assignment_count={armed_assignment_count} member_id={original_member_id}\n",
            encoding="utf-8",
        )

        deadline = time.monotonic() + 60
        while True:
            if time.monotonic() >= deadline:
                fail("timed out waiting to rejoin the replacement broker")
            message = poll_record(survivor)
            if message is not None:
                fail(
                    f"replayed {topic}[{message.partition()}]@{message.offset()} "
                    "before replacement rejoin"
                )
            replacement_member_id = survivor.memberid()
            if (
                survivor_tracker["count"] > armed_assignment_count
                and assignment_partitions(survivor_tracker, topic) == PARTITIONS
                and replacement_member_id
                and replacement_member_id != original_member_id
            ):
                break

        rejoined.write_text(
            f"member_id={replacement_member_id} assignment={sorted(PARTITIONS)}\n",
            encoding="utf-8",
        )
        produce_generation(bootstrap, topic, second_value)
        consume_generation([survivor], topic, second_value, 2)
        require_committed(survivor, topic, 3)
    finally:
        if departing is not None:
            departing.close()
        survivor.close()
        for marker in (ready, survivor_ready, armed):
            marker.unlink(missing_ok=True)

    print(
        f"librdkafka {libversion()[0]} split {topic}[0..2] across two members, "
        "reassigned all partitions to one retained survivor, then resumed each "
        "partition at offsets 2 and committed next offsets 3 after broker replacement"
    )


if __name__ == "__main__":
    main()
