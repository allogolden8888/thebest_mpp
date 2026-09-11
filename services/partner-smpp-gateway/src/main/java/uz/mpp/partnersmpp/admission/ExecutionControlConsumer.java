package uz.mpp.partnersmpp.admission;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-replica manual-assignment consumer for the compacted
 * {@code execution.control} topic.
 *
 * <p>There is intentionally no consumer-group balancing: every SMPP gateway
 * replica must own a complete GLOBAL/PARTNER snapshot. Reconnects rebuild a
 * replacement snapshot from the beginning while the previous confirmed view
 * remains active (fail-static).</p>
 */
public final class ExecutionControlConsumer implements AutoCloseable {

    public static final String TOPIC = "execution.control";
    private static final Duration METADATA_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration PARTITION_REFRESH_INTERVAL = Duration.ofSeconds(30);

    private final String bootstrapServers;
    private final ExecutionControlSnapshot snapshot;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile KafkaConsumer<byte[], byte[]> activeConsumer;
    private Thread worker;

    public ExecutionControlConsumer(String bootstrapServers, ExecutionControlSnapshot snapshot) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("Kafka bootstrap servers must not be empty");
        }
        this.bootstrapServers = bootstrapServers;
        this.snapshot = snapshot;
    }

    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        worker = Thread.ofPlatform()
            .name("execution-control-consumer")
            .daemon(true)
            .start(this::runLoop);
    }

    private void runLoop() {
        while (running.get()) {
            try {
                consumeSession();
            } catch (WakeupException e) {
                if (running.get()) {
                    System.err.println("execution.control consumer wakeup; reconnecting: " + e.getMessage());
                }
            } catch (Exception e) {
                if (running.get()) {
                    System.err.println("execution.control consumer failed; last confirmed snapshot remains active: " + e);
                }
            }

            if (running.get()) {
                try {
                    Thread.sleep(2_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void consumeSession() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // group.id is a Kafka client requirement only. Manual assign below
        // means replicas neither rebalance nor split partitions.
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "partner-smpp-gateway-control-" + UUID.randomUUID());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "partner-smpp-gateway-control-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            activeConsumer = consumer;
            List<PartitionInfo> metadata = consumer.partitionsFor(TOPIC, METADATA_TIMEOUT);
            if (metadata == null || metadata.isEmpty()) {
                throw new IllegalStateException("execution.control has no partitions");
            }
            Set<TopicPartition> assignment = new HashSet<>();
            for (PartitionInfo partition : metadata) {
                assignment.add(new TopicPartition(TOPIC, partition.partition()));
            }

            consumer.assign(assignment);
            consumer.seekToBeginning(assignment);
            Map<TopicPartition, Long> replayEnds = consumer.endOffsets(assignment, METADATA_TIMEOUT);
            ExecutionControlSnapshot.Replay replay = new ExecutionControlSnapshot.Replay();
            boolean live = false;
            long nextPartitionRefresh = System.nanoTime() + PARTITION_REFRESH_INTERVAL.toNanos();

            while (running.get()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (live) {
                        snapshot.applyLive(record.key(), record.value());
                    } else {
                        replay.apply(record.key(), record.value());
                    }
                }

                if (!live) {
                    Map<TopicPartition, Long> positions = new HashMap<>();
                    for (TopicPartition partition : assignment) {
                        positions.put(partition, consumer.position(partition, METADATA_TIMEOUT));
                    }
                    if (caughtUp(replayEnds, positions)) {
                        snapshot.install(replay);
                        live = true;
                        System.out.println("initial execution.control snapshot fully replayed; ready=" + snapshot.isReady());
                    }
                }

                if (System.nanoTime() >= nextPartitionRefresh) {
                    Set<TopicPartition> current = new HashSet<>();
                    for (PartitionInfo partition : consumer.partitionsFor(TOPIC, METADATA_TIMEOUT)) {
                        current.add(new TopicPartition(TOPIC, partition.partition()));
                    }
                    if (!current.equals(assignment)) {
                        throw new IllegalStateException("execution.control partition set changed; rebuilding full snapshot");
                    }
                    nextPartitionRefresh = System.nanoTime() + PARTITION_REFRESH_INTERVAL.toNanos();
                }
            }
        } finally {
            activeConsumer = null;
        }
    }

    static boolean caughtUp(Map<TopicPartition, Long> ends, Map<TopicPartition, Long> positions) {
        if (!positions.keySet().containsAll(ends.keySet())) {
            return false;
        }
        for (Map.Entry<TopicPartition, Long> end : ends.entrySet()) {
            if (positions.get(end.getKey()) < end.getValue()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        KafkaConsumer<byte[], byte[]> consumer = activeConsumer;
        if (consumer != null) {
            consumer.wakeup();
        }
        if (worker != null && worker != Thread.currentThread()) {
            try {
                worker.join(5_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
