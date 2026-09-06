"""Retain one real librdkafka producer across a committed-but-unacknowledged batch."""
import pathlib
import sys
import time
from confluent_kafka import Consumer, Producer, TopicPartition, libversion

bootstrap, topic, directory, expired = sys.argv[1:]
state = pathlib.Path(directory)
producer = Producer({'bootstrap.servers': bootstrap, 'enable.idempotence': True,
                     'compression.type': 'none', 'acks': 'all', 'linger.ms': 0,
                     'message.timeout.ms': 180000, 'request.timeout.ms': 10000})


def send(value, offset):
    delivered = []
    producer.produce(topic, partition=0, value=value, on_delivery=lambda error, message: delivered.append((error, message.offset())))
    assert producer.flush(150) == 0
    assert delivered == [(None, offset)], delivered
    print(f'librdkafka {libversion()} topic={topic} delivered={value} offset={offset}', flush=True)


send('first', 0)
(state / (topic + '.delivered')).touch()
deadline = time.monotonic() + 120
while not (state / (topic + '.continue')).exists():
    assert time.monotonic() < deadline
    time.sleep(0.05)
send('next', 1)
consumer = Consumer({'bootstrap.servers': bootstrap, 'group.id': topic,
                     'enable.auto.commit': False, 'auto.offset.reset': 'earliest'})
consumer.assign([TopicPartition(topic, 0, 1 if expired == 'yes' else 0)])
expected = [(1, b'next')] if expired == 'yes' else [(0, b'first'), (1, b'next')]
seen = []
deadline = time.monotonic() + 30
while len(seen) < len(expected) and time.monotonic() < deadline:
    message = consumer.poll(1)
    if message is not None:
        assert message.error() is None, message.error()
        seen.append((message.offset(), message.value()))
assert seen == expected, seen
assert consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10) == (1 if expired == 'yes' else 0, 2)
consumer.close()
print(f'librdkafka topic={topic} exact readback={seen}', flush=True)
