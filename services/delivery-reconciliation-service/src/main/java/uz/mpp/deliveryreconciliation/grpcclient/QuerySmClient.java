package uz.mpp.deliveryreconciliation.grpcclient;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import uz.mpp.deliveryreconciliation.core.Evidence;
import uz.mpp.platformcontracts.grpc.v1.OperatorQuerySmServiceGrpc;
import uz.mpp.platformcontracts.grpc.v1.QuerySmRequest;
import uz.mpp.platformcontracts.grpc.v1.QuerySmResponse;

import java.util.concurrent.TimeUnit;

/**
 * call_query_sm (service_internal_methods.md §1.9) — instance-addressed
 * gRPC на Operator SMPP Session Manager (resolve_gateway_instance резолвит
 * endpoint через Runtime Redis registry, здесь принимается уже резолвленным).
 * Реальный gRPC-клиент, не проверялся против живого Operator SMPP Session
 * Manager в этой песочнице.
 */
public final class QuerySmClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final OperatorQuerySmServiceGrpc.OperatorQuerySmServiceBlockingStub stub;

    public QuerySmClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = OperatorQuerySmServiceGrpc.newBlockingStub(channel);
    }

    QuerySmClient(OperatorQuerySmServiceGrpc.OperatorQuerySmServiceBlockingStub stub) {
        this.channel = null;
        this.stub = stub;
    }

    public Evidence.QuerySmOutcome query(String messageId, String operatorId, String smscMessageId) {
        QuerySmResponse resp = stub.querySm(QuerySmRequest.newBuilder()
            .setMessageId(messageId).setOperatorId(operatorId).setSmscMessageId(smscMessageId)
            .build());
        return mapResponse(resp);
    }

    static Evidence.QuerySmOutcome mapResponse(QuerySmResponse resp) {
        if (!resp.getFound()) {
            // "не найдено" из-за приоритета submit_sm (DEFERRED_...) —
            // неубедительно, не путать с "оператор подтвердил, что не находил".
            if (resp.getRawStatus().startsWith("DEFERRED")) {
                return Evidence.QuerySmOutcome.INCONCLUSIVE;
            }
            return Evidence.QuerySmOutcome.CONFIRMED_NOT_FOUND;
        }
        return Evidence.QuerySmOutcome.CONFIRMED_DELIVERED;
    }

    @Override
    public void close() {
        if (channel != null) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}