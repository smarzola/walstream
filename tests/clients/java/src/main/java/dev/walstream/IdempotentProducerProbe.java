package dev.walstream;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

/** Same producer survives a lost acknowledgement and broker replacement. */
public final class IdempotentProducerProbe {
    public static void main(String[] args) throws Exception {
        String bootstrap = args[0], topic = args[1];
        Path state = Path.of(args[2]);
        boolean expired = args[3].equals("yes");
        Properties config = new Properties();
        config.put("bootstrap.servers", bootstrap);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("enable.idempotence", "true");
        config.put("compression.type", "none");
        config.put("acks", "all");
        config.put("linger.ms", "0");
        config.put("delivery.timeout.ms", "180000");
        config.put("request.timeout.ms", "10000");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            long offset = producer.send(new ProducerRecord<>(topic, 0, null, "first")).get(150, TimeUnit.SECONDS).offset();
            if (offset != 0) throw new AssertionError("retry offset=" + offset);
            System.out.println("Java 4.2.0 topic=" + topic + " first delivery offset=" + offset);
            Files.createFile(state.resolve(topic + ".delivered"));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            while (!Files.exists(state.resolve(topic + ".continue"))) {
                if (System.nanoTime() > deadline) throw new AssertionError("continue timeout");
                Thread.sleep(50);
            }
            offset = producer.send(new ProducerRecord<>(topic, 0, null, "next")).get(30, TimeUnit.SECONDS).offset();
            if (offset != 1) throw new AssertionError("next offset=" + offset);
        }
        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", bootstrap);
        consumerConfig.put("group.id", topic);
        consumerConfig.put("enable.auto.commit", "false");
        consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig)) {
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seek(partition, expired ? 1 : 0);
            List<String> expected = expired ? List.of("1:next") : List.of("0:first", "1:next");
            List<String> seen = new ArrayList<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (seen.size() < expected.size() && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofSeconds(1)).forEach(record -> seen.add(record.offset() + ":" + record.value()));
            }
            if (!seen.equals(expected) || consumer.endOffsets(List.of(partition)).get(partition) != 2L
                    || consumer.beginningOffsets(List.of(partition)).get(partition) != (expired ? 1L : 0L)) {
                throw new AssertionError("incorrect readback " + seen);
            }
            System.out.println("Java 4.2.0 topic=" + topic + " exact readback=" + seen);
        }
    }
}
