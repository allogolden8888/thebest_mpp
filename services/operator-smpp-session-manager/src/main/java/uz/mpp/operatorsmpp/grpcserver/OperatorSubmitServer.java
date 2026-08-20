package uz.mpp.operatorsmpp.grpcserver;

import io.grpc.stub.StreamObserver;
import uz.mpp.operatorsmpp.client.OperatorSmppClient;
import uz.mpp.operatorsmpp.codec.CommandStatus;
import uz.mpp.operatorsmpp.codec.Pdu;
import uz.mpp.operatorsmpp.codec.ShortMessagePdu;
import uz.mpp.operatorsmpp.codec.ShortMessagePduResp;
import uz.mpp.operatorsmpp.core.PacerMetrics;
import uz.mpp.operatorsmpp.core.PriorityGate;
import uz.mpp.operatorsmpp.core.PriorityTier;
import uz.mpp.operatorsmpp.kafkaio.OperatorEventPublisher;
import uz.mpp.platformcontracts.common.v1.Protocol;
import uz.mpp.platformcontracts.events.v1.OperatorSubmitAccepted;
import uz.mpp.platformcontracts.grpc.v1.*;

import com.google.protobuf.Timestamp;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * handle_submit_command + enforce_window_and_tps + send_submit_sm +
 * handle_submit_sm_resp + publish_submit_accepted + reply_submit_result
 * (service_internal_methods.md §1.3) — instance-addressed gRPC от Delivery
 * Service (platform-contracts/grpc/operator_gateway.proto).
 *
 * <p>dynamic-seeking-russell.md "Priority-tier scheduler": {@link #submit}
 * НЕ вызывает {@code client.submitSm()} напрямую и не блокирует
 * gRPC-поток — классифицирует приоритет, кладёт {@link QueuedSubmit} в
 * bounded {@code ArrayBlockingQueue} своего tier'а (неблокирующе) и
 * возвращается немедленно. Реальный SMPP round-trip — в {@link #dispatchOne},
 * вызываемом позже worker-пулом из Main.java, когда {@code PacerCore}
 * (тоже в Main.java, эта фаза "не знает" про приоритет/очереди) решит
 * допустить элемент к отправке.
 */
public final class OperatorSubmitServer extends OperatorSubmitServiceGrpc.OperatorSubmitServiceImplBase {

    private static final long SUBMIT_TIMEOUT_MS = 5000;

    /**
     * DLR SLA оператора — от него считается correlation_expires_at, по
     * которому dlr-correlation-writer/dlr-manager решают, когда DLR уже
     * можно не ждать (сообщение уходит в DELIVERY_UNRESOLVED). 4 часа —
     * то же допущение, что capacity_model.md §1 использует для расчёта
     * объёма dlr_correlation; реальный SLA per-operator, при заведении
     * второго оператора это станет per-route настройкой, не константой.
     */
    private static final Duration DEFAULT_DLR_CORRELATION_TTL = Duration.ofHours(4);

    private final Duration dlrCorrelationTtl = DEFAULT_DLR_CORRELATION_TTL;
    private final OperatorSmppClient client;
    private final Map<PriorityTier, ArrayBlockingQueue<QueuedSubmit>> queuesByTier;
    private final Map<PriorityTier, Integer> maxQueueDepthByTier;
    private final PriorityGate priorityGate;
    private final OperatorEventPublisher eventPublisher;
    private final PacerMetrics metrics;
    private final long submitTimeoutMs;

    public OperatorSubmitServer(OperatorSmppClient client,
                                 Map<PriorityTier, ArrayBlockingQueue<QueuedSubmit>> queuesByTier,
                                 Map<PriorityTier, Integer> maxQueueDepthByTier,
                                 PriorityGate priorityGate, OperatorEventPublisher eventPublisher,
                                 PacerMetrics metrics) {
        this(client, queuesByTier, maxQueueDepthByTier, priorityGate, eventPublisher, metrics, SUBMIT_TIMEOUT_MS);
    }

    /**
     * Тестовый конструктор (CODE_REVIEW.md finding #2, тест ambiguous-таймаута) — короткий
     * {@code submitTimeoutMs}, чтобы не ждать реальные 5с {@link #SUBMIT_TIMEOUT_MS} в тестах.
     */
    public OperatorSubmitServer(OperatorSmppClient client,
                                 Map<PriorityTier, ArrayBlockingQueue<QueuedSubmit>> queuesByTier,
                                 Map<PriorityTier, Integer> maxQueueDepthByTier,
                                 PriorityGate priorityGate, OperatorEventPublisher eventPublisher,
                                 PacerMetrics metrics, long submitTimeoutMs) {
        this.client = client;
        this.queuesByTier = queuesByTier;
        this.maxQueueDepthByTier = maxQueueDepthByTier;
        this.priorityGate = priorityGate;
        this.eventPublisher = eventPublisher;
        this.metrics = metrics;
        this.submitTimeoutMs = submitTimeoutMs;
    }

    @Override
    public void submit(SubmitRequest request, StreamObserver<SubmitResponse> responseObserver) {
        PriorityTier tier = PriorityTier.forPriorityFlag(request.getPriorityFlag());
        ArrayBlockingQueue<QueuedSubmit> queue = queuesByTier.get(tier);
        int maxDepth = maxQueueDepthByTier.getOrDefault(tier, Integer.MAX_VALUE);

        // Логическая ёмкость проверяется явно (а не через ArrayBlockingQueue
        // capacity напрямую) — ArrayBlockingQueue не может иметь ёмкость 0,
        // а safety-valve намеренно должен уметь это (MAX_QUEUE_DEPTH_PER_TIER=0
        // в тестах — форсирует немедленный PACER_QUEUE_FULL для любого запроса).
        // Реальная Java-ёмкость очереди в Main.java всегда >= 1 и заведомо
        // не меньше maxDepth — она здесь только как техническое ограничение
        // JDK, не как содержательный предел.
        if (queue.size() >= maxDepth || !queue.offer(new QueuedSubmit(request, responseObserver, System.currentTimeMillis()))) {
            metrics.recordRejected(tier, "PACER_QUEUE_FULL");
            sendRejected(responseObserver, "PACER_QUEUE_FULL");
            return;
        }
    }

    /**
     * Реальный SMPP submit — вызывается из worker-пула (Main.java) после
     * того, как {@code PacerCore} допустил этот элемент к отправке в этот
     * тик. Раньше это было тело {@code submit()} целиком; теперь это тело
     * дёрнутое из очереди и выполняемое ВНЕ gRPC-потока.
     */
    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }

    public void dispatchOne(QueuedSubmit queued) {
        SubmitRequest request = queued.request();
        StreamObserver<SubmitResponse> responseObserver = queued.responseObserver();

        priorityGate.submitStarted();
        try {
            String smscMessageId = "";
            for (MessageSegment segment : request.getSegmentsList()) {
                // priority_flag (SMPP 3.4 §5.2.14, 0-3, 3=наивысший) —
                // прокидывается на провод без изменений, но защитно
                // клампится: partner-rest-receiver уже валидирует 0-3 на
                // границе, но это gRPC-контракт между сервисами, не
                // доверяем ему слепо на случай рассинхронизации версий.
                byte priorityFlag = (byte) Math.max(0, Math.min(3, request.getPriorityFlag()));
                ShortMessagePdu body = new ShortMessagePdu(
                    "", (byte) 0, (byte) 1, "",
                    (byte) 0, (byte) 1, request.getDestinationAddress(),
                    (byte) 0, (byte) 0, priorityFlag,
                    (byte) 1, (byte) 0, dataCodingFor(segment.getEncoding()), (byte) 0,
                    segment.getContent().toByteArray()
                );

                Pdu resp;
                try {
                    resp = client.submitSm(body, submitTimeoutMs);
                } catch (TimeoutException e) {
                    reply(responseObserver, SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_AMBIGUOUS, "", "SUBMIT_TIMEOUT");
                    return;
                }

                if (resp.header().commandStatus() != CommandStatus.ESME_ROK) {
                    reply(responseObserver, SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_REJECTED, "",
                        "SMPP_STATUS_0x" + Integer.toHexString(resp.header().commandStatus()));
                    return;
                }
                smscMessageId = ((ShortMessagePduResp) resp.body()).messageId();
            }

            // submitted_at/segment_id/correlation_expires_at раньше НЕ
            // заполнялись вообще — реальный баг, найденный вживую:
            // submitted_at оставался protobuf-нулём (epoch 1970), а это
            // ключ партиционирования dlr.dlr_correlation, поэтому
            // dlr-correlation-writer падал на КАЖДОЙ вставке с
            // "no partition of relation dlr_correlation found for row"
            // (партиций на 1970 год нет и быть не должно). Таблица
            // корреляции оставалась пустой -> dlr-manager не мог
            // сопоставить ни один DLR -> delivery.status пустой ->
            // сообщения навсегда в SUBMITTED.
            //
            // segment_id=1: тот же номер, что проставляет DLR-сторона
            // (см. Main.java) — ключ корреляции обязан сойтись с обеих
            // сторон. Мультисегментные сообщения здесь схлопываются в
            // один smsc_message_id (цикл выше перезаписывает его на
            // каждом сегменте) — это существующее ограничение, не
            // вносится этой правкой.
            Instant submittedAt = Instant.now();
            OperatorSubmitAccepted event = OperatorSubmitAccepted.newBuilder()
                .setMessageId(request.getMessageId())
                .setStageExecutionId(request.getStageExecutionId())
                .setOperatorId(request.getOperatorId())
                .setProtocol(Protocol.PROTOCOL_SMPP)
                .setSmscMessageId(smscMessageId)
                .setSegmentId(1)
                .setSubmittedAt(toTimestamp(submittedAt))
                .setCorrelationExpiresAt(toTimestamp(submittedAt.plus(dlrCorrelationTtl)))
                .build();
            eventPublisher.publishSubmitAccepted(event);

            reply(responseObserver, SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_ACCEPTED, smscMessageId, "");
        } catch (Exception e) {
            reply(responseObserver, SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_AMBIGUOUS, "", "UNEXPECTED_ERROR: " + e.getMessage());
        } finally {
            priorityGate.submitFinished();
        }
    }

    /**
     * SMPP 3.4 §5.2.19 data_coding: 0x00 — SMSC Default Alphabet (GSM-7),
     * 0x08 — UCS2. Реальная находка (прогон против живого SMSC, не статичное
     * чтение): раньше dataCoding был захардкожен в {@code (byte) 0} независимо
     * от {@code MessageSegment.encoding} ("GSM7"/"UCS2", уже корректно
     * посчитанного delivery-service/SegmentMessage.java) — SMSC получал
     * настоящие UTF-16BE-байты кириллицы, но помеченные как GSM-7 default
     * alphabet, и декодировал побайтово: старший байт (0x04 для кириллицы)
     * терялся, младший — совпадал с ASCII-диапазоном и отображался как
     * читаемая, но бессмысленная латиница.
     */
    private static byte dataCodingFor(String encoding) {
        return "UCS2".equals(encoding) ? (byte) 0x08 : (byte) 0x00;
    }

    /** Для safety-valve max-wait (Main.java, PACER_QUEUE_TIMEOUT) — тот же формат ответа, что и остальные reject'ы. */
    public static void sendRejected(StreamObserver<SubmitResponse> observer, String reasonCode) {
        observer.onNext(SubmitResponse.newBuilder()
            .setStatus(SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_REJECTED)
            .setReasonCode(reasonCode)
            .build());
        observer.onCompleted();
    }

    private void reply(StreamObserver<SubmitResponse> observer, SubmitOutcomeStatus status, String smscMessageId, String reasonCode) {
        observer.onNext(SubmitResponse.newBuilder()
            .setStatus(status)
            .setSmscMessageId(smscMessageId)
            .setReasonCode(reasonCode)
            .build());
        observer.onCompleted();
    }
}
