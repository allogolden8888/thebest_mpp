package uz.mpp.operatorsmpp.grpcserver;

import io.grpc.stub.StreamObserver;
import uz.mpp.operatorsmpp.core.PriorityGate;
import uz.mpp.platformcontracts.grpc.v1.OperatorQuerySmServiceGrpc;
import uz.mpp.platformcontracts.grpc.v1.QuerySmRequest;
import uz.mpp.platformcontracts.grpc.v1.QuerySmResponse;

/**
 * enforce_query_sm_priority + handle_query_sm_command (service_internal_methods.md
 * §1.3) — опциональный RPC от Reconciliation. submit_sm имеет приоритет.
 *
 * <p><b>Известное ограничение</b>: сам SMPP {@code query_sm} PDU не входит в
 * подмножество {@code smpp-codec} этого среза (см.
 * services/partner-smpp-gateway и {@code codec/CommandId} — только bind/
 * submit/deliver/enquire_link/unbind/generic_nack). Этот сервер проверяет
 * приоритет (`enforce_query_sm_priority`) реально, но фактическую отправку
 * query_sm оператору не выполняет — всегда возвращает `found=false` при
 * `PERMIT`, задокументировано как открытый пробел, не скрыто.
 */
public final class OperatorQuerySmServer extends OperatorQuerySmServiceGrpc.OperatorQuerySmServiceImplBase {

    private final PriorityGate priorityGate;

    public OperatorQuerySmServer(PriorityGate priorityGate) {
        this.priorityGate = priorityGate;
    }

    @Override
    public void querySm(QuerySmRequest request, StreamObserver<QuerySmResponse> responseObserver) {
        if (priorityGate.checkQuerySm() == PriorityGate.QueryDecision.DEFER) {
            responseObserver.onNext(QuerySmResponse.newBuilder()
                .setFound(false)
                .setRawStatus("DEFERRED_SUBMIT_PRIORITY")
                .build());
            responseObserver.onCompleted();
            return;
        }

        // query_sm PDU не реализован в codec этого среза — см. докстринг класса.
        responseObserver.onNext(QuerySmResponse.newBuilder()
            .setFound(false)
            .setRawStatus("QUERY_SM_NOT_IMPLEMENTED")
            .build());
        responseObserver.onCompleted();
    }
}
