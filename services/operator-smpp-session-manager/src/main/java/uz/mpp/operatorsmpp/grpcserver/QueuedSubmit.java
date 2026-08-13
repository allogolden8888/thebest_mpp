package uz.mpp.operatorsmpp.grpcserver;

import io.grpc.stub.StreamObserver;
import uz.mpp.platformcontracts.grpc.v1.SubmitRequest;
import uz.mpp.platformcontracts.grpc.v1.SubmitResponse;

/**
 * Единица очереди пейсера (dynamic-seeking-russell.md "Priority-tier
 * scheduler") — {@code submit()} больше не блокирует gRPC-поток на
 * {@code client.submitSm()}: строит {@code QueuedSubmit} и кладёт его в
 * bounded {@code ArrayBlockingQueue} своего tier'а (неблокирующе,
 * {@code offer}), возвращается немедленно. Реальный SMPP submit и
 * {@code responseObserver.onNext/onCompleted} происходят позже, из
 * worker-пула, когда пейсер (Main.java, {@code PacerCore}) решит
 * допустить этот элемент — валидное использование gRPC: ответ не обязан
 * приходить из потока, обработавшего запрос.
 *
 * @param enqueuedAtEpochMs момент постановки в очередь — используется
 *                          для safety valve max-wait (см.
 *                          {@code MAX_QUEUE_WAIT_MS}, {@code PACER_QUEUE_TIMEOUT}).
 */
public record QueuedSubmit(
    SubmitRequest request,
    StreamObserver<SubmitResponse> responseObserver,
    long enqueuedAtEpochMs
) {
}
