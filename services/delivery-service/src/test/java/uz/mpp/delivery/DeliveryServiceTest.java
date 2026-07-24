package uz.mpp.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import uz.mpp.delivery.DeliveryService.SubmitOutcome;
import uz.mpp.delivery.MessageContextStore.MessageContext;
import uz.mpp.delivery.SegmentMessage.Segment;
import uz.mpp.platformcontracts.common.v1.DeliveryExtension;
import uz.mpp.platformcontracts.common.v1.Outcome;
import uz.mpp.platformcontracts.common.v1.Protocol;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;
import uz.mpp.platformcontracts.common.v1.StageName;
import uz.mpp.platformcontracts.grpc.v1.SubmitOutcomeStatus;
import uz.mpp.platformcontracts.grpc.v1.SubmitRequest;
import uz.mpp.platformcontracts.grpc.v1.SubmitResponse;

class DeliveryServiceTest {

    private StageExecuteCommand command() {
        return StageExecuteCommand.newBuilder()
            .setEventId("e1")
            .setMessageId("m1")
            .setStageName(StageName.STAGE_NAME_DELIVERY)
            .setStageExecutionId("se1")
            .setAttempt(1)
            .setTraceparent("tp1")
            .setDelivery(DeliveryExtension.newBuilder()
                .setRouteId("beeline_smpp_primary")
                .setProtocol(Protocol.PROTOCOL_SMPP)
                .setResolvedOperatorId("beeline"))
            .build();
    }

    @Test
    void acceptedResponseInterpretedAsSucceeded() {
        SubmitResponse response = SubmitResponse.newBuilder()
            .setStatus(SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_ACCEPTED)
            .setSmscMessageId("smsc-123")
            .build();
        SubmitOutcome outcome = DeliveryService.interpretSubmitResult(response);
        assertEquals(Outcome.OUTCOME_SUCCEEDED, outcome.outcome());
        assertEquals("smsc-123", outcome.smscMessageId());
    }

    @Test
    void rejectedResponseInterpretedAsFailedWithReasonCode() {
        SubmitResponse response = SubmitResponse.newBuilder()
            .setStatus(SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_REJECTED)
            .setReasonCode("INVALID_DESTINATION")
            .build();
        SubmitOutcome outcome = DeliveryService.interpretSubmitResult(response);
        assertEquals(Outcome.OUTCOME_FAILED, outcome.outcome());
        assertEquals("INVALID_DESTINATION", outcome.reasonCode());
    }

    @Test
    void ambiguousResponseInterpretedAsSubmissionOutcomeUnknown() {
        SubmitResponse response = SubmitResponse.newBuilder()
            .setStatus(SubmitOutcomeStatus.SUBMIT_OUTCOME_STATUS_AMBIGUOUS)
            .build();
        SubmitOutcome outcome = DeliveryService.interpretSubmitResult(response);
        assertEquals(Outcome.OUTCOME_SUBMISSION_OUTCOME_UNKNOWN, outcome.outcome());
    }

    @Test
    void grpcTransportFailureAlsoMapsToSubmissionOutcomeUnknownNotFailed() {
        // Не FAILED — нельзя утверждать, что оператор бы отклонил, обрыв
        // мог случиться уже после того, как оператор реально принял submit.
        SubmitOutcome outcome = DeliveryService.handleGrpcFailure("UNAVAILABLE");
        assertEquals(Outcome.OUTCOME_SUBMISSION_OUTCOME_UNKNOWN, outcome.outcome());
        assertEquals("UNAVAILABLE", outcome.reasonCode());
    }

    @Test
    void buildSubmitRequestCarriesOperatorIdFromDeliveryExtensionNotRouteId() {
        // Прямая регрессия: DeliveryExtension.resolved_operator_id — новое
        // поле (platform-contracts/common/stage_contract.proto field 4),
        // добавленное именно из-за этого сервиса — до него operator_id было
        // неоткуда взять.
        MessageContext ctx = new MessageContext("hello", "Click", "998901331835", "GSM7");
        List<Segment> segments = SegmentMessage.segment(ctx.body(), ctx.encoding());
        SubmitRequest request = DeliveryService.buildSubmitRequest(command(), command().getDelivery(), ctx, "q1", segments);
        assertEquals("beeline", request.getOperatorId());
        assertEquals("beeline_smpp_primary", request.getRouteId());
        assertEquals("998901331835", request.getDestinationAddress());
        assertEquals("q1", request.getQueueMsgId());
        assertEquals(1, request.getSegmentsCount());
    }

    @Test
    void buildEventCarriesQueueMsgIdInDeliveryResult() {
        StageCompletedEvent event = DeliveryService.buildEvent(command(), "q1", new SubmitOutcome(Outcome.OUTCOME_SUCCEEDED, "", "smsc-1"));
        assertEquals(Outcome.OUTCOME_SUCCEEDED, event.getOutcome());
        assertEquals("q1", event.getDelivery().getQueueMsgId());
        assertEquals("se1", event.getStageExecutionId());
        assertFalse(event.getRetryable(), "SUCCEEDED не должен быть помечен retryable");
    }

    @Test
    void buildEventMarksNonSucceededAsRetryable() {
        StageCompletedEvent event = DeliveryService.buildEvent(command(), "q1", new SubmitOutcome(Outcome.OUTCOME_SUBMISSION_OUTCOME_UNKNOWN, "TIMEOUT", ""));
        assertTrue(event.getRetryable());
        assertEquals("TIMEOUT", event.getReasonCode());
    }

    @Test
    void admitDecisionAllowsSubmit() {
        assertTrue(DeliveryService.isAdmitted(ControlSnapshot.Decision.ADMIT));
        assertFalse(DeliveryService.isAdmitted(ControlSnapshot.Decision.HOLD));
    }
}
