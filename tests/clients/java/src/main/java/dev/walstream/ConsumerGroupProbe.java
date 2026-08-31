package dev.walstream;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
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
    private static final Set<Integer> PARTITIONS = Set.of(0, 1, 2);

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
        Path survivorReady = stateDirectory.resolve("java.survivor");
        Path arm = stateDirectory.resolve("arm");
        Path armed = stateDirectory.resolve("java.armed");
        Path rejoined = stateDirectory.resolve("java.rejoined");

        produceGeneration(bootstrap, topic, "seed");
        AssignmentTracker initializerTracker = new AssignmentTracker();
        try (KafkaConsumer<String, String> initializer =
                consumer(bootstrap, group, "earliest")) {
            initializer.subscribe(List.of(topic), initializerTracker);
            consumeGeneration(List.of(initializer), topic, "seed", 0);
            if (!partitionIds(initializerTracker.assignment(), topic).equals(PARTITIONS)) {
                throw new IllegalStateException(
                        "seed consumer did not own all three partitions");
            }
            requireCommitted(initializer, topic, 1);
        }

        AssignmentTracker survivorTracker = new AssignmentTracker();
        AssignmentTracker departingTracker = new AssignmentTracker();
        KafkaConsumer<String, String> survivor = consumer(bootstrap, group, "none");
        KafkaConsumer<String, String> departing = consumer(bootstrap, group, "none");
        survivor.subscribe(List.of(topic), survivorTracker);
        departing.subscribe(List.of(topic), departingTracker);
        try {
            List<Set<Integer>> initialAssignments =
                    waitForSplit(
                            List.of(survivor, departing),
                            List.of(survivorTracker, departingTracker),
                            topic);
            produceGeneration(bootstrap, topic, firstValue);
            consumeGeneration(List.of(survivor, departing), topic, firstValue, 1);
            requireCommitted(survivor, topic, 2);
            Files.writeString(
                    ready,
                    "split=" + initialAssignments.get(0) + "|" + initialAssignments.get(1)
                            + " committed=2,2,2\n");

            int assignmentCountBeforeLeave = survivorTracker.assignmentCount();
            departing.close();
            departing = null;
            waitForSurvivor(survivor, survivorTracker, topic, assignmentCountBeforeLeave);
            String originalMemberId = survivor.groupMetadata().memberId();
            if (originalMemberId == null || originalMemberId.isEmpty()) {
                throw new IllegalStateException(
                        "surviving consumer had no member ID before replacement");
            }
            Files.writeString(
                    survivorReady,
                    "member_id=" + originalMemberId + " assignment=" + PARTITIONS + "\n");

            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (!Files.exists(arm)) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException(
                            "timed out waiting to arm replacement recovery");
                }
                requireNoRecords(
                        survivor.poll(Duration.ofMillis(250)),
                        "while broker was being replaced");
            }

            int armedAssignmentCount = survivorTracker.assignmentCount();
            Files.writeString(
                    armed,
                    "assignment_count=" + armedAssignmentCount
                            + " member_id=" + originalMemberId + "\n");

            deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            String replacementMemberId;
            while (true) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException(
                            "timed out waiting to rejoin the replacement broker");
                }
                requireNoRecords(
                        survivor.poll(Duration.ofMillis(250)),
                        "before replacement rejoin");
                replacementMemberId = survivor.groupMetadata().memberId();
                if (survivorTracker.assignmentCount() > armedAssignmentCount
                        && partitionIds(survivorTracker.assignment(), topic).equals(PARTITIONS)
                        && replacementMemberId != null
                        && !replacementMemberId.isEmpty()
                        && !replacementMemberId.equals(originalMemberId)) {
                    break;
                }
            }

            Files.writeString(
                    rejoined,
                    "member_id=" + replacementMemberId + " assignment=" + PARTITIONS + "\n");
            produceGeneration(bootstrap, topic, secondValue);
            consumeGeneration(List.of(survivor), topic, secondValue, 2);
            requireCommitted(survivor, topic, 3);
        } finally {
            if (departing != null) {
                departing.close();
            }
            survivor.close();
            Files.deleteIfExists(ready);
            Files.deleteIfExists(survivorReady);
            Files.deleteIfExists(armed);
        }

        System.out.printf(
                "Apache Kafka Java client 4.2.0 split %s[0..2] across two members, "
                        + "reassigned all partitions to one retained survivor, then resumed each "
                        + "partition at offsets 2 and committed next offsets 3 after broker replacement%n",
                topic);
    }

    private static List<Set<Integer>> waitForSplit(
            List<KafkaConsumer<String, String>> consumers,
            List<AssignmentTracker> trackers,
            String topic) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            for (KafkaConsumer<String, String> consumer : consumers) {
                requireNoRecords(
                        consumer.poll(Duration.ofMillis(250)),
                        "before initial production");
            }
            Set<Integer> first = partitionIds(trackers.get(0).assignment(), topic);
            Set<Integer> second = partitionIds(trackers.get(1).assignment(), topic);
            Set<Integer> union = new HashSet<>(first);
            union.addAll(second);
            if (!first.isEmpty()
                    && !second.isEmpty()
                    && disjoint(first, second)
                    && union.equals(PARTITIONS)) {
                return List.of(Set.copyOf(first), Set.copyOf(second));
            }
        }
        throw new IllegalStateException(
                "timed out waiting for a disjoint complete three-partition split");
    }

    private static void waitForSurvivor(
            KafkaConsumer<String, String> survivor,
            AssignmentTracker tracker,
            String topic,
            int previousAssignmentCount) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            requireNoRecords(
                    survivor.poll(Duration.ofMillis(250)),
                    "during survivor reassignment");
            if (tracker.assignmentCount() > previousAssignmentCount
                    && partitionIds(tracker.assignment(), topic).equals(PARTITIONS)) {
                return;
            }
        }
        throw new IllegalStateException(
                "timed out waiting for the survivor to own all three partitions");
    }

    private static void consumeGeneration(
            List<KafkaConsumer<String, String>> consumers,
            String topic,
            String valuePrefix,
            long expectedOffset) {
        Map<Integer, OwnedRecord> seen = new HashMap<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline && !seen.keySet().equals(PARTITIONS)) {
            for (KafkaConsumer<String, String> consumer : consumers) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    int partition = record.partition();
                    if (!record.topic().equals(topic) || !PARTITIONS.contains(partition)) {
                        throw new IllegalStateException(
                                "unexpected record source: "
                                        + record.topic() + "[" + partition + "]");
                    }
                    if (record.offset() != expectedOffset) {
                        throw new IllegalStateException(
                                "expected " + topic + "[" + partition + "]@" + expectedOffset
                                        + ", received @" + record.offset());
                    }
                    String expectedValue = valuePrefix + "-" + partition;
                    if (!record.value().equals(expectedValue)) {
                        throw new IllegalStateException(
                                "expected value " + expectedValue + ", received " + record.value());
                    }
                    if (seen.putIfAbsent(partition, new OwnedRecord(consumer, record)) != null) {
                        throw new IllegalStateException(
                                "received duplicate " + topic + "[" + partition + "]@"
                                        + expectedOffset);
                    }
                }
            }
        }
        if (!seen.keySet().equals(PARTITIONS)) {
            throw new IllegalStateException(
                    "timed out waiting for every partition at offset " + expectedOffset
                            + "; received " + seen.keySet());
        }
        for (Map.Entry<Integer, OwnedRecord> entry : seen.entrySet()) {
            TopicPartition partition = new TopicPartition(topic, entry.getKey());
            entry.getValue().consumer().commitSync(
                    Map.of(partition, new OffsetAndMetadata(expectedOffset + 1)));
        }
    }

    private static void requireCommitted(
            KafkaConsumer<String, String> consumer,
            String topic,
            long expectedNextOffset) {
        Set<TopicPartition> partitions = Set.of(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2));
        Map<TopicPartition, OffsetAndMetadata> committed =
                consumer.committed(partitions, Duration.ofSeconds(10));
        for (TopicPartition partition : partitions) {
            OffsetAndMetadata offset = committed.get(partition);
            if (offset == null || offset.offset() != expectedNextOffset) {
                throw new IllegalStateException(
                        "expected committed next offset " + expectedNextOffset + " for "
                                + partition + ", received " + offset);
            }
        }
    }

    private static void produceGeneration(
            String bootstrap,
            String topic,
            String valuePrefix) throws Exception {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "1");
        producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        producerProperties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15_000);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            for (int partition : PARTITIONS) {
                producer.send(
                                new ProducerRecord<>(
                                        topic,
                                        partition,
                                        null,
                                        valuePrefix + "-" + partition))
                        .get(15, TimeUnit.SECONDS);
            }
        }
    }

    private static KafkaConsumer<String, String> consumer(
            String bootstrap,
            String group,
            String offsetReset) {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        consumerProperties.put(
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                RangeAssignor.class.getName());
        consumerProperties.put("group.protocol", "classic");
        consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
        consumerProperties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
        return new KafkaConsumer<>(consumerProperties);
    }

    private static Set<Integer> partitionIds(Set<TopicPartition> assignment, String topic) {
        Set<Integer> partitions = new HashSet<>();
        for (TopicPartition topicPartition : assignment) {
            if (!topicPartition.topic().equals(topic)) {
                throw new IllegalStateException(
                        "unexpected topic assignment: " + topicPartition);
            }
            partitions.add(topicPartition.partition());
        }
        return partitions;
    }

    private static boolean disjoint(Set<Integer> first, Set<Integer> second) {
        Set<Integer> overlap = new HashSet<>(first);
        overlap.retainAll(second);
        return overlap.isEmpty();
    }

    private static void requireNoRecords(
            ConsumerRecords<String, String> records,
            String phase) {
        if (!records.isEmpty()) {
            ConsumerRecord<String, String> record = records.iterator().next();
            throw new IllegalStateException(
                    "received unexpected record " + record.topic() + "[" + record.partition()
                            + "]@" + record.offset() + " " + phase);
        }
    }

    private record OwnedRecord(
            KafkaConsumer<String, String> consumer,
            ConsumerRecord<String, String> record) {}

    private static final class AssignmentTracker implements ConsumerRebalanceListener {
        private final AtomicInteger count = new AtomicInteger();
        private final AtomicReference<Set<TopicPartition>> assignment =
                new AtomicReference<>(Set.of());

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            assignment.set(Set.copyOf(partitions));
            count.incrementAndGet();
        }

        int assignmentCount() {
            return count.get();
        }

        Set<TopicPartition> assignment() {
            return assignment.get();
        }
    }
}
