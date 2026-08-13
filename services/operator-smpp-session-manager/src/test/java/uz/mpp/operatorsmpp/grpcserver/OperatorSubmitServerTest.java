package uz.mpp.operatorsmpp.grpcserver;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import uz.mpp.operatorsmpp.client.FakeSmscServer;
import uz.mpp.operatorsmpp.client.OperatorSmppClient;
import uz.mpp.operatorsmpp.core.PacerMetrics;
import uz.mpp.operatorsmpp.core.PriorityGate;
import uz.mpp.operatorsmpp.core.PriorityTier;
import uz.mpp.operatorsmpp.kafkaio.OperatorEventPublisher;
import uz.mpp.platformcontracts.grpc.v1.*;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Сквозной путь handle_submit_command: реальный gRPC-хендлер ->
 * реальный OperatorSmppClient -> реальный TCP round-trip с FakeSmscServer.
 * Только Kafka замокан (MockProducer, официальный test double kafka-clients).
 *
 * <p>dynamic-seeking-russell.md "Priority-tier scheduler": {@code submit()}
 * теперь только кладёт элемент в очередь своего tier'а (неблокирующе) —
 * тесты, проверяющие реальный SMPP round-trip, вызывают
 * {@link OperatorSubmitServer#dispatchOne} напрямую (то же самое, что
 * раньше делал сам {@code submit()} целиком), эмулируя то, что в
 * production делает pacer-worker-пул из Main.java.
 */
class OperatorSubmitServerTest {

    private FakeSmscServer smsc;
    private OperatorSmppClient client;

    @AfterEach
    void tearDown() {
        if (client != null) client.close();
        if (smsc != null) smsc.stop();
    }

    private static class CapturingObserver implements StreamObserver<SubmitResponse> {
        final CompletableFuture<SubmitResponse> future = new CompletableFuture<>();

        @Override
        public void onNext(SubmitResponse value) {
            future.complete(value);
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
        }
    }

    /** Дефолтная ёмкость очереди — заведомо не тесная, чтобы не мешать тестам, не проверяющим PACER_QUEUE_FULL. */
    private static Map<PriorityTier, ArrayBlockingQueue<QueuedSubmit>> queuesWithCapacity(int capacity) {
        Map<PriorityTier, ArrayBlockingQueue<QueuedSubmit>> queues = new EnumMap<>(PriorityTier.class);
        for (PriorityTier tier : PriorityTier.values()) {
            queues.put(tier, new ArrayBlockingQueue<>(Math.max(1, capacity)));
        }
        return queues;
    }

    private static Map<PriorityTier, Integer> maxDepth(int depth) {
        Map<PriorityTier, Integer> maxDepthByTier = new EnumMap<>(PriorityTier.class);
        for (PriorityTier tier : PriorityTier.values()) {
            maxDepthByTier.put(tier, depth);
        }
        return maxDepthByTier;
    }

    @Test
    void submitAcceptedPublishesEventAndReturnsSmscMessageId() throws Exception {
        smsc = new FakeSmscServer();
        int port = smsc.start();
        client = new OperatorSmppClient(null);
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);

        MockProducer<String, byte[]> mockProducer =
            new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
        OperatorEventPublisher eventPublisher = new OperatorEventPublisher(mockProducer);

        OperatorSubmitServer server = new OperatorSubmitServer(
            client, queuesWithCapacity(100), maxDepth(100), new PriorityGate(100), eventPublisher, new PacerMetrics());

        SubmitRequest request = SubmitRequest.newBuilder()
            .setMessageId("msg-1").setStageExecutionId("exec-1").setOperatorId("beeline_uz")
            .setDestinationAddress("998901234567")
            .addSegments(MessageSegment.newBuilder().setSegmentId(1).setContent(ByteString.copyFromUtf8("hello")).setEncoding("GSM7"))
            .build();

        CapturingObserver observer = new CapturingObserver();
        server.submit(request, observer);
        // submit() больше не блокирует и не отправляет напрямую — в production
        // это делает pacer-worker-пул (Main.java) после PacerCore.decide().
        // Здесь эмулируем ровно этот шаг вручную, на реальном SMPP round-trip.
        server.dispatchOne(new QueuedSubmit(request, observer, System.currentTimeMillis()));

        SubmitResponse resp = observer.future.get();
        assertEquals(SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_ACCEPTED, resp.getStatus());
        assertEquals("smsc-msg-1", resp.getSmscMessageId());
        assertEquals(1, mockProducer.history().size());
        assertEquals("operator.submit.accepted", mockProducer.history().get(0).topic());
    }

    @Test
    void submitSetsUcs2DataCodingForCyrillicSegment() throws Exception {
        // Реальная находка (прогон против живого SMSC, не статичное чтение): dataCoding
        // был захардкожен в 0 независимо от MessageSegment.encoding — SMSC получал
        // настоящие UTF-16BE-байты кириллицы, помеченные как GSM-7, и декодировал их
        // побайтово (искажённый текст в реальном тесте). Проверяем то, что реально
        // пришло по TCP в FakeSmscServer, а не то, что клиент думал, что отправил.
        smsc = new FakeSmscServer();
        int port = smsc.start();
        client = new OperatorSmppClient(null);
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);

        MockProducer<String, byte[]> mockProducer =
            new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
        OperatorEventPublisher eventPublisher = new OperatorEventPublisher(mockProducer);

        OperatorSubmitServer server = new OperatorSubmitServer(
            client, queuesWithCapacity(100), maxDepth(100), new PriorityGate(100), eventPublisher, new PacerMetrics());

        byte[] ucs2Bytes = "Спасибо".getBytes(java.nio.charset.StandardCharsets.UTF_16BE);
        SubmitRequest request = SubmitRequest.newBuilder()
            .setMessageId("msg-1").setStageExecutionId("exec-1").setOperatorId("beeline_uz")
            .setDestinationAddress("998901234567")
            .addSegments(MessageSegment.newBuilder().setSegmentId(1)
                .setContent(ByteString.copyFrom(ucs2Bytes)).setEncoding("UCS2"))
            .build();

        CapturingObserver observer = new CapturingObserver();
        server.submit(request, observer);
        server.dispatchOne(new QueuedSubmit(request, observer, System.currentTimeMillis()));

        SubmitResponse resp = observer.future.get();
        assertEquals(SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_ACCEPTED, resp.getStatus());
        assertEquals((byte) 0x08, smsc.lastSubmitSm().dataCoding(), "UCS2 сегмент должен нести data_coding=0x08, не 0x00");
        org.junit.jupiter.api.Assertions.assertArrayEquals(ucs2Bytes, smsc.lastSubmitSm().shortMessage(),
            "байты на проводе должны совпадать с тем, что построил delivery-service/SegmentMessage.java");
    }

    @Test
    void submitSetsGsm7DataCodingForPlainAsciiSegment() throws Exception {
        smsc = new FakeSmscServer();
        int port = smsc.start();
        client = new OperatorSmppClient(null);
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);

        MockProducer<String, byte[]> mockProducer =
            new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
        OperatorEventPublisher eventPublisher = new OperatorEventPublisher(mockProducer);

        OperatorSubmitServer server = new OperatorSubmitServer(
            client, queuesWithCapacity(100), maxDepth(100), new PriorityGate(100), eventPublisher, new PacerMetrics());

        SubmitRequest request = SubmitRequest.newBuilder()
            .setMessageId("msg-1").setStageExecutionId("exec-1").setOperatorId("beeline_uz")
            .setDestinationAddress("998901234567")
            .addSegments(MessageSegment.newBuilder().setSegmentId(1)
                .setContent(ByteString.copyFromUtf8("hello")).setEncoding("GSM7"))
            .build();

        CapturingObserver observer = new CapturingObserver();
        server.submit(request, observer);
        server.dispatchOne(new QueuedSubmit(request, observer, System.currentTimeMillis()));

        observer.future.get();
        assertEquals((byte) 0x00, smsc.lastSubmitSm().dataCoding());
    }

    @Test
    void submitRejectedWhenPacerQueueFull() throws Exception {
        smsc = new FakeSmscServer();
        int port = smsc.start();
        client = new OperatorSmppClient(null);
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);

        MockProducer<String, byte[]> mockProducer =
            new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
        OperatorEventPublisher eventPublisher = new OperatorEventPublisher(mockProducer);

        // MAX_QUEUE_DEPTH_PER_TIER=0 — логический предел проверяется явно
        // (queue.size() >= maxDepth) до offer(), форсирует немедленный
        // PACER_QUEUE_FULL для любого запроса (см. OperatorSubmitServer.submit()
        // javadoc — ArrayBlockingQueue не может иметь реальную ёмкость 0,
        // поэтому Java-ёмкость очереди всё равно >= 1, а предел — отдельное поле).
        OperatorSubmitServer server = new OperatorSubmitServer(
            client, queuesWithCapacity(1), maxDepth(0), new PriorityGate(100), eventPublisher, new PacerMetrics());

        SubmitRequest request = SubmitRequest.newBuilder()
            .setMessageId("msg-1").setOperatorId("beeline_uz").setDestinationAddress("998901234567")
            .addSegments(MessageSegment.newBuilder().setSegmentId(1).setContent(ByteString.copyFromUtf8("hi")))
            .build();

        CapturingObserver observer = new CapturingObserver();
        server.submit(request, observer);

        SubmitResponse resp = observer.future.get();
        assertEquals(SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_REJECTED, resp.getStatus());
        assertEquals("PACER_QUEUE_FULL", resp.getReasonCode());
        assertEquals(0, mockProducer.history().size(), "отклонённый пейсером submit не должен публиковать submit_accepted");
    }

    @Test
    void submitTimesOutToAmbiguousAndDoesNotLeakPendingResponseEntry() throws Exception {
        // Закрывает пробел, отмеченный в CODE_REVIEW.md ("ни один тест не проверяет
        // pendingResponses после таймаута") на реальном production-пути: gRPC handle_submit_command
        // -> OperatorSmppClient -> FakeSmscServer, а не только на низкоуровневом клиенте.
        smsc = new FakeSmscServer();
        int port = smsc.start();
        smsc.setDropSubmitResponses(true); // тихая потеря ответа SMSC (finding #2)
        client = new OperatorSmppClient(null);
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);

        MockProducer<String, byte[]> mockProducer =
            new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
        OperatorEventPublisher eventPublisher = new OperatorEventPublisher(mockProducer);

        // короткий submitTimeoutMs — не ждём реальные 5с SUBMIT_TIMEOUT_MS
        OperatorSubmitServer server = new OperatorSubmitServer(
            client, queuesWithCapacity(100), maxDepth(100), new PriorityGate(100), eventPublisher, new PacerMetrics(), 300);

        SubmitRequest request = SubmitRequest.newBuilder()
            .setMessageId("msg-1").setOperatorId("beeline_uz").setDestinationAddress("998901234567")
            .addSegments(MessageSegment.newBuilder().setSegmentId(1).setContent(ByteString.copyFromUtf8("hi")))
            .build();

        CapturingObserver observer = new CapturingObserver();
        server.submit(request, observer);
        server.dispatchOne(new QueuedSubmit(request, observer, System.currentTimeMillis()));

        SubmitResponse resp = observer.future.get(3, TimeUnit.SECONDS);
        assertEquals(SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_AMBIGUOUS, resp.getStatus());
        assertEquals("SUBMIT_TIMEOUT", resp.getReasonCode());
        assertEquals(0, mockProducer.history().size(), "timeout не должен публиковать submit_accepted");
        assertEquals(0, client.pendingResponseCount(),
            "CODE_REVIEW.md HIGH #2: pendingResponses не должен утекать после timeout на реальном submit-пути");
    }

    @Test
    void submitClassifiesIntoHighTierQueueForPriorityFlag3() throws Exception {
        smsc = new FakeSmscServer();
        int port = smsc.start();
        client = new OperatorSmppClient(null);
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);

        MockProducer<String, byte[]> mockProducer =
            new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
        OperatorEventPublisher eventPublisher = new OperatorEventPublisher(mockProducer);

        Map<PriorityTier, ArrayBlockingQueue<QueuedSubmit>> queues = queuesWithCapacity(100);
        OperatorSubmitServer server = new OperatorSubmitServer(
            client, queues, maxDepth(100), new PriorityGate(100), eventPublisher, new PacerMetrics());

        SubmitRequest request = SubmitRequest.newBuilder()
            .setMessageId("msg-1").setOperatorId("beeline_uz").setDestinationAddress("998901234567")
            .setPriorityFlag(3)
            .addSegments(MessageSegment.newBuilder().setSegmentId(1).setContent(ByteString.copyFromUtf8("hi")))
            .build();

        server.submit(request, new CapturingObserver());

        assertEquals(1, queues.get(PriorityTier.HIGH).size(), "priority_flag=3 должен попасть в очередь HIGH");
        assertEquals(0, queues.get(PriorityTier.MEDIUM).size());
        assertEquals(0, queues.get(PriorityTier.LOW).size());
    }

    @Test
    void submitClassifiesIntoLowTierQueueForDefaultPriorityFlag() throws Exception {
        smsc = new FakeSmscServer();
        int port = smsc.start();
        client = new OperatorSmppClient(null);
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);

        MockProducer<String, byte[]> mockProducer =
            new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
        OperatorEventPublisher eventPublisher = new OperatorEventPublisher(mockProducer);

        Map<PriorityTier, ArrayBlockingQueue<QueuedSubmit>> queues = queuesWithCapacity(100);
        OperatorSubmitServer server = new OperatorSubmitServer(
            client, queues, maxDepth(100), new PriorityGate(100), eventPublisher, new PacerMetrics());

        // priority_flag не выставлен -> 0 -> LOW (см. PriorityTier.forPriorityFlag).
        SubmitRequest request = SubmitRequest.newBuilder()
            .setMessageId("msg-1").setOperatorId("beeline_uz").setDestinationAddress("998901234567")
            .addSegments(MessageSegment.newBuilder().setSegmentId(1).setContent(ByteString.copyFromUtf8("hi")))
            .build();

        server.submit(request, new CapturingObserver());

        assertEquals(1, queues.get(PriorityTier.LOW).size());
        assertEquals(0, queues.get(PriorityTier.HIGH).size());
        assertEquals(0, queues.get(PriorityTier.MEDIUM).size());
    }

    @Test
    void dispatchOneClampsOutOfRangePriorityFlagBeforeBuildingPdu() throws Exception {
        // Защитный кламп 0-3 (gRPC-контракт между сервисами, не доверяем ему
        // слепо) — здесь просто проверяем, что submit всё равно проходит
        // сквозной путь без исключения при "битом" priority_flag.
        smsc = new FakeSmscServer();
        int port = smsc.start();
        client = new OperatorSmppClient(null);
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);

        MockProducer<String, byte[]> mockProducer =
            new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
        OperatorEventPublisher eventPublisher = new OperatorEventPublisher(mockProducer);

        OperatorSubmitServer server = new OperatorSubmitServer(
            client, queuesWithCapacity(100), maxDepth(100), new PriorityGate(100), eventPublisher, new PacerMetrics());

        SubmitRequest request = SubmitRequest.newBuilder()
            .setMessageId("msg-1").setOperatorId("beeline_uz").setDestinationAddress("998901234567")
            .setPriorityFlag(99) // out-of-range на проводе — PriorityTier классифицирует как LOW, dispatchOne клампит PDU-поле в 0-3
            .addSegments(MessageSegment.newBuilder().setSegmentId(1).setContent(ByteString.copyFromUtf8("hi")))
            .build();

        CapturingObserver observer = new CapturingObserver();
        server.dispatchOne(new QueuedSubmit(request, observer, System.currentTimeMillis()));

        SubmitResponse resp = observer.future.get();
        assertEquals(SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_ACCEPTED, resp.getStatus());
    }
}
