package uz.mpp.operatorsmpp.grpcserver;

import io.grpc.stub.StreamObserver;
import uz.mpp.operatorsmpp.client.OperatorSmppClient;
import uz.mpp.operatorsmpp.codec.CommandStatus;
import uz.mpp.operatorsmpp.codec.Pdu;
import uz.mpp.operatorsmpp.codec.ShortMessagePdu;
import uz.mpp.operatorsmpp.codec.ShortMessagePduResp;
import uz.mpp.operatorsmpp.core.PriorityGate;
import uz.mpp.operatorsmpp.core.TokenBucket;
import uz.mpp.operatorsmpp.kafkaio.OperatorEventPublisher;
import uz.mpp.platformcontracts.common.v1.Protocol;
import uz.mpp.platformcontracts.events.v1.OperatorSubmitAccepted;
import uz.mpp.platformcontracts.grpc.v1.*;

import java.util.concurrent.TimeoutException;

/**
 * handle_submit_command + enforce_window_and_tps + send_submit_sm +
 * handle_submit_sm_resp + publish_submit_accepted + reply_submit_result
 * (service_internal_methods.md §1.3) — instance-addressed gRPC от Delivery
 * Service (platform-contracts/grpc/operator_gateway.proto).
 */
public final class OperatorSubmitServer extends OperatorSubmitServiceGrpc.OperatorSubmitServiceImplBase {

    private static final long SUBMIT_TIMEOUT_MS = 5000;

    private final OperatorSmppClient client;
    private final TokenBucket tpsBucket;
    private final PriorityGate priorityGate;
    private final OperatorEventPublisher eventPublisher;

    public OperatorSubmitServer(OperatorSmppClient client, TokenBucket tpsBucket,
                                 PriorityGate priorityGate, OperatorEventPublisher eventPublisher) {
        this.client = client;
        this.tpsBucket = tpsBucket;
        this.priorityGate = priorityGate;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void submit(SubmitRequest request, StreamObserver<SubmitResponse> responseObserver) {
        if (!tpsBucket.tryAcquire(System.currentTimeMillis())) {
            responseObserver.onNext(SubmitResponse.newBuilder()
                .setStatus(SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_REJECTED)
                .setReasonCode("TPS_THROTTLED")
                .build());
            responseObserver.onCompleted();
            return;
        }

        priorityGate.submitStarted();
        try {
            String smscMessageId = "";
            for (MessageSegment segment : request.getSegmentsList()) {
                ShortMessagePdu body = new ShortMessagePdu(
                    "", (byte) 0, (byte) 1, "",
                    (byte) 0, (byte) 1, request.getDestinationAddress(),
                    (byte) 0, (byte) 0, (byte) 0,
                    (byte) 1, (byte) 0, (byte) 0, (byte) 0,
                    segment.getContent().toByteArray()
                );

                Pdu resp;
                try {
                    resp = client.submitSm(body, SUBMIT_TIMEOUT_MS);
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

            OperatorSubmitAccepted event = OperatorSubmitAccepted.newBuilder()
                .setMessageId(request.getMessageId())
                .setStageExecutionId(request.getStageExecutionId())
                .setOperatorId(request.getOperatorId())
                .setProtocol(Protocol.PROTOCOL_SMPP)
                .setSmscMessageId(smscMessageId)
                .build();
            eventPublisher.publishSubmitAccepted(event);

            reply(responseObserver, SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_ACCEPTED, smscMessageId, "");
        } catch (Exception e) {
            reply(responseObserver, SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_AMBIGUOUS, "", "UNEXPECTED_ERROR: " + e.getMessage());
        } finally {
            priorityGate.submitFinished();
        }
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
