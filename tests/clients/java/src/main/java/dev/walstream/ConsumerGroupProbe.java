package dev.walstream;

import java.time.Duration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class ConsumerGroupProbe {
    private ConsumerGroupProbe() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException(
                    "usage: BOOTSTRAP TOPIC GROUP FIRST SECOND STATE_DIR");
        }
        String bootstrap = args[0];
        String topic = args[1];
        String group = args[2];
        String firstValue = args[3];
        String secondValue = args[4];
        Path stateDirectory = Path.of(args[5]);
        Path ready = stateDirectory.resolve("java.ready");
        Path proceed = stateDirectory.resolve("proceed");

        produce(bootstrap, topic, firstValue);
        TopicPartition partition = new TopicPartition(topic, 0);
        try {
            try (KafkaConsumer<String, String> consumer = consumer(bootstrap, group)) {
                consumer.subscribe(List.of(topic));
                ConsumerRecord<String, String> first =
                        consumeExact(consumer, topic, firstValue, 0);
                commitExact(consumer, partition, 0);
                Files.writeString(ready, "committed offset 1\n");

                long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
                while (!Files.exists(proceed)) {
                    if (System.nanoTime() >= deadline) {
                        throw new IllegalStateException("timed out waiting for broker replacement");
                    }
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(250));
                    if (!records.isEmpty()) {
                        ConsumerRecord<String, String> unexpected = records.iterator().next();
                        throw new IllegalStateException(
                                "received unexpected record while broker was being replaced: offset "
                                        + unexpected.offset());
                    }
                }

                produce(bootstrap, topic, secondValue);
                ConsumerRecord<String, String> second =
                        consumeExact(consumer, topic, secondValue, 1);
                commitExact(consumer, partition, 1);
            }
        } finally {
            Files.deleteIfExists(ready);
        }

        System.out.printf(
                "Apache Kafka Java client 4.2.0 kept one consumer alive across replacement, "
                        + "then consumed and committed %s[0]@0 and @1%n",
                topic);
    }

    private static void produce(String bootstrap, String topic, String value) throws Exception {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "1");
        producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        producerProperties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15_000);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            producer.send(new ProducerRecord<>(topic, value)).get(15, TimeUnit.SECONDS);
        }
    }

    private static KafkaConsumer<String, String> consumer(String bootstrap, String group) {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                RangeAssignor.class.getName());
        consumerProperties.put("group.protocol", "classic");
        consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
        consumerProperties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
        return new KafkaConsumer<>(consumerProperties);
    }

    private static ConsumerRecord<String, String> consumeExact(
            KafkaConsumer<String, String> consumer,
            String topic,
            String value,
            long expectedOffset) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty()) {
                continue;
            }
            ConsumerRecord<String, String> found = records.iterator().next();
            if (!found.topic().equals(topic) || found.partition() != 0) {
                throw new IllegalStateException(
                        "unexpected assignment: " + found.topic() + "[" + found.partition() + "]");
            }
            if (found.offset() != expectedOffset) {
                throw new IllegalStateException(
                        "expected offset " + expectedOffset + ", received " + found.offset());
            }
            if (!found.value().equals(value)) {
                throw new IllegalStateException(
                        "expected value " + value + ", received " + found.value());
            }
            return found;
        }
        throw new IllegalStateException(
                "timed out waiting for " + topic + "[0]@" + expectedOffset);
    }

    private static void commitExact(
            KafkaConsumer<String, String> consumer,
            TopicPartition partition,
            long expectedOffset) {
        consumer.commitSync();
        Map<TopicPartition, OffsetAndMetadata> committed =
                consumer.committed(Set.of(partition), Duration.ofSeconds(10));
        long committedOffset = committed.get(partition).offset();
        if (committedOffset != expectedOffset + 1) {
            throw new IllegalStateException(
                    "expected committed next offset " + (expectedOffset + 1)
                            + ", received " + committedOffset);
        }
    }
}
