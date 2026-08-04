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
import uz.mpp.operatorsmpp.core.PriorityGate;
import uz.mpp.operatorsmpp.core.TokenBucket;
import uz.mpp.operatorsmpp.kafkaio.OperatorEventPublisher;
import uz.mpp.platformcontracts.grpc.v1.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Сквозной путь handle_submit_command: реальный gRPC-хендлер ->
 * реальный OperatorSmppClient -> реальный TCP round-trip с FakeSmscServer.
 * Только Kafka замокан (MockProducer, официальный test double kafka-clients).
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
            client, new TokenBucket(100, 100, System.currentTimeMillis()), new PriorityGate(100), eventPublisher);

        SubmitRequest request = SubmitRequest.newBuilder()
            .setMessageId("msg-1").setStageExecutionId("exec-1").setOperatorId("beeline_uz")
            .setDestinationAddress("998901234567")
            .addSegments(MessageSegment.newBuilder().setSegmentId(1).setContent(ByteString.copyFromUtf8("hello")).setEncoding("GSM7"))
            .build();

        CapturingObserver observer = new CapturingObserver();
        server.submit(request, observer);

        SubmitResponse resp = observer.future.get();
        assertEquals(SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_ACCEPTED, resp.getStatus());
        assertEquals("smsc-msg-1", resp.getSmscMessageId());
        assertEquals(1, mockProducer.history().size());
        assertEquals("operator.submit.accepted", mockProducer.history().get(0).topic());
    }

    @Test
    void submitRejectedWhenTpsThrottled() throws Exception {
        smsc = new FakeSmscServer();
        int port = smsc.start();
        client = new OperatorSmppClient(null);
        client.connect("127.0.0.1", port);
        client.bind("mpp_esme", "s3cr3t", "", 2000);

        MockProducer<String, byte[]> mockProducer =
            new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
        OperatorEventPublisher eventPublisher = new OperatorEventPublisher(mockProducer);

        // capacity=0 — немедленно throttled
        OperatorSubmitServer server = new OperatorSubmitServer(
            client, new TokenBucket(0, 0, System.currentTimeMillis()), new PriorityGate(100), eventPublisher);

        SubmitRequest request = SubmitRequest.newBuilder()
            .setMessageId("msg-1").setOperatorId("beeline_uz").setDestinationAddress("998901234567")
            .addSegments(MessageSegment.newBuilder().setSegmentId(1).setContent(ByteString.copyFromUtf8("hi")))
            .build();

        CapturingObserver observer = new CapturingObserver();
        server.submit(request, observer);

        SubmitResponse resp = observer.future.get();
        assertEquals(SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_REJECTED, resp.getStatus());
        assertEquals("TPS_THROTTLED", resp.getReasonCode());
        assertEquals(0, mockProducer.history().size(), "throttled submit не должен публиковать submit_accepted");
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
            client, new TokenBucket(100, 100, System.currentTimeMillis()), new PriorityGate(100), eventPublisher, 300);

        SubmitRequest request = SubmitRequest.newBuilder()
            .setMessageId("msg-1").setOperatorId("beeline_uz").setDestinationAddress("998901234567")
            .addSegments(MessageSegment.newBuilder().setSegmentId(1).setContent(ByteString.copyFromUtf8("hi")))
            .build();

        CapturingObserver observer = new CapturingObserver();
        server.submit(request, observer);

        SubmitResponse resp = observer.future.get(3, TimeUnit.SECONDS);
        assertEquals(SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_AMBIGUOUS, resp.getStatus());
        assertEquals("SUBMIT_TIMEOUT", resp.getReasonCode());
        assertEquals(0, mockProducer.history().size(), "timeout не должен публиковать submit_accepted");
        assertEquals(0, client.pendingResponseCount(),
            "CODE_REVIEW.md HIGH #2: pendingResponses не должен утекать после timeout на реальном submit-пути");
    }
}
