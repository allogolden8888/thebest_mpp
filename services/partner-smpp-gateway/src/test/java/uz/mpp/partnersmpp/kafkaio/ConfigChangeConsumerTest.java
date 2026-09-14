package uz.mpp.partnersmpp.kafkaio;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.ConfigEntityType;
import uz.mpp.platformcontracts.events.v1.ConfigChangeEvent;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MockConsumer} — официальный in-memory test double из kafka-clients
 * (тот же приём, что {@code IncomingPublisherTest} использует {@code
 * MockProducer}) — {@link ConfigChangeConsumer#processBatch} прогоняется
 * без реального брокера.
 */
class ConfigChangeConsumerTest {

    private static final TopicPartition TP = new TopicPartition(ConfigChangeConsumer.TOPIC, 0);

    private static ConsumerRecords<String, byte[]> singleRecordBatch(long offset, String key, byte[] value) {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(ConfigChangeConsumer.TOPIC, 0, offset, key, value);
        return new ConsumerRecords<>(Map.of(TP, List.of(record)));
    }

    private static byte[] partnerEvent(String partnerId, String status) {
        return ConfigChangeEvent.newBuilder()
            .setEntityType(ConfigEntityType.CONFIG_ENTITY_TYPE_PARTNER)
            .setEntityId(partnerId)
            .setVersion(1)
            .setStatus(status)
            .build()
            .toByteArray();
    }

    private static byte[] nonPartnerEvent() {
        return ConfigChangeEvent.newBuilder()
            .setEntityType(ConfigEntityType.CONFIG_ENTITY_TYPE_ROUTING_TABLE)
            .setEntityId("beeline_uz")
            .setVersion(1)
            .setStatus("active")
            .build()
            .toByteArray();
    }

    private MockConsumer<String, byte[]> mockConsumer() {
        MockConsumer<String, byte[]> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mock.assign(List.of(TP));
        mock.updateBeginningOffsets(Map.of(TP, 0L));
        return mock;
    }

    @Test
    void partnerEventTriggersRefreshAndCommitsOffset() {
        MockConsumer<String, byte[]> mock = mockConsumer();
        List<Map.Entry<String, String>> refreshed = new ArrayList<>();
        ConfigChangeConsumer consumer = new ConfigChangeConsumer(mock, event -> refreshed.add(new AbstractMap.SimpleEntry<>(event.getEntityId(), event.getStatus())));

        consumer.processBatch(singleRecordBatch(0, "acme", partnerEvent("acme", "active")));

        assertEquals(List.of(Map.entry("acme", "active")), refreshed);
        assertEquals(1L, mock.committed(java.util.Set.of(TP)).get(TP).offset());
    }

    @Test
    void archivedPartnerEventPassesArchivedStatusThrough() {
        // Архивация должна дойти полным событием: config:current не
        // переставляется projector'ом на archived-версию, а tombstone с
        // version fence должен остаться в локальном snapshot.
        MockConsumer<String, byte[]> mock = mockConsumer();
        List<Map.Entry<String, String>> refreshed = new ArrayList<>();
        ConfigChangeConsumer consumer = new ConfigChangeConsumer(mock, event -> refreshed.add(new AbstractMap.SimpleEntry<>(event.getEntityId(), event.getStatus())));

        consumer.processBatch(singleRecordBatch(0, "acme", partnerEvent("acme", "archived")));

        assertEquals(List.of(Map.entry("acme", "archived")), refreshed);
        assertEquals(1L, mock.committed(java.util.Set.of(TP)).get(TP).offset());
    }

    @Test
    void nonPartnerEventIsIgnoredButOffsetStillCommitted() {
        MockConsumer<String, byte[]> mock = mockConsumer();
        List<Map.Entry<String, String>> refreshed = new ArrayList<>();
        ConfigChangeConsumer consumer = new ConfigChangeConsumer(mock, event -> refreshed.add(new AbstractMap.SimpleEntry<>(event.getEntityId(), event.getStatus())));

        consumer.processBatch(singleRecordBatch(0, "beeline_uz", nonPartnerEvent()));

        assertTrue(refreshed.isEmpty());
        assertEquals(1L, mock.committed(java.util.Set.of(TP)).get(TP).offset());
    }

    @Test
    void malformedPayloadIsSkippedAndCommitted() {
        MockConsumer<String, byte[]> mock = mockConsumer();
        List<Map.Entry<String, String>> refreshed = new ArrayList<>();
        ConfigChangeConsumer consumer = new ConfigChangeConsumer(mock, event -> refreshed.add(new AbstractMap.SimpleEntry<>(event.getEntityId(), event.getStatus())));

        consumer.processBatch(singleRecordBatch(0, "bad", new byte[] { (byte) 0xFF, 0x00, 0x01 }));

        assertTrue(refreshed.isEmpty());
        assertEquals(1L, mock.committed(java.util.Set.of(TP)).get(TP).offset(),
            "poison-сообщение коммитится (пропускается), не блокирует партицию навечно");
    }

    @Test
    void tombstoneIsSkippedAndCommitted() {
        MockConsumer<String, byte[]> mock = mockConsumer();
        List<Map.Entry<String, String>> refreshed = new ArrayList<>();
        ConfigChangeConsumer consumer = new ConfigChangeConsumer(mock, event -> refreshed.add(new AbstractMap.SimpleEntry<>(event.getEntityId(), event.getStatus())));

        consumer.processBatch(singleRecordBatch(0, "acme", null));

        assertTrue(refreshed.isEmpty());
        assertEquals(1L, mock.committed(java.util.Set.of(TP)).get(TP).offset());
    }

    @Test
    void refreshFailureDoesNotCommitAndStopsRestOfPartitionThisBatch() {
        MockConsumer<String, byte[]> mock = mockConsumer();
        AtomicInteger calls = new AtomicInteger();
        ConfigChangeConsumer consumer = new ConfigChangeConsumer(mock, event -> {
            calls.incrementAndGet();
            throw new RuntimeException("Redis unreachable");
        });

        ConsumerRecord<String, byte[]> first = new ConsumerRecord<>(ConfigChangeConsumer.TOPIC, 0, 0, "acme", partnerEvent("acme", "active"));
        ConsumerRecord<String, byte[]> second = new ConsumerRecord<>(ConfigChangeConsumer.TOPIC, 0, 1, "beta", partnerEvent("beta", "active"));
        consumer.processBatch(new ConsumerRecords<>(Map.of(TP, List.of(first, second))));

        assertEquals(1, calls.get(), "вторая запись той же партиции не должна обрабатываться в этом поллинге после первой ошибки");
        Map<TopicPartition, OffsetAndMetadata> committed = mock.committed(java.util.Set.of(TP));
        assertTrue(committed.get(TP) == null, "ни один offset этой партиции не должен закоммититься при ошибке на первой записи");
        assertEquals(0L, mock.position(TP), "consumer обязан повторить упавшую запись без restart/rebalance");
    }

    @Test
    void partialSuccessCommitsUpToLastGoodOffset() {
        MockConsumer<String, byte[]> mock = mockConsumer();
        ConfigChangeConsumer consumer = new ConfigChangeConsumer(mock, event -> {
            if (event.getEntityId().equals("bad")) {
                throw new RuntimeException("boom");
            }
        });

        ConsumerRecord<String, byte[]> ok1 = new ConsumerRecord<>(ConfigChangeConsumer.TOPIC, 0, 0, "acme", partnerEvent("acme", "active"));
        ConsumerRecord<String, byte[]> ok2 = new ConsumerRecord<>(ConfigChangeConsumer.TOPIC, 0, 1, "beta", partnerEvent("beta", "active"));
        ConsumerRecord<String, byte[]> bad = new ConsumerRecord<>(ConfigChangeConsumer.TOPIC, 0, 2, "bad", partnerEvent("bad", "active"));
        consumer.processBatch(new ConsumerRecords<>(Map.of(TP, List.of(ok1, ok2, bad))));

        assertEquals(2L, mock.committed(java.util.Set.of(TP)).get(TP).offset(),
            "должны закоммититься первые две успешные записи (offset двух записей = следующий offset после второй)");
        assertEquals(2L, mock.position(TP), "следующий poll обязан начаться с первой упавшей записи");
    }

    @Test
    void initialReplayRequiresEveryCapturedPartitionAtItsEnd() {
        TopicPartition second = new TopicPartition(ConfigChangeConsumer.TOPIC, 1);
        Map<TopicPartition, Long> ends = Map.of(TP, 10L, second, 4L);

        assertTrue(ConfigChangeConsumer.caughtUp(ends, Map.of(TP, 10L, second, 4L)));
        assertTrue(!ConfigChangeConsumer.caughtUp(ends, Map.of(TP, 10L, second, 3L)));
        assertTrue(!ConfigChangeConsumer.caughtUp(ends, Map.of(TP, 10L)));
    }
}
