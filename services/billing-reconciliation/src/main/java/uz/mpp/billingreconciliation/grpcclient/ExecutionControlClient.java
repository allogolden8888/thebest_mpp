package uz.mpp.billingreconciliation.grpcclient;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import uz.mpp.platformcontracts.common.v1.ExecutionControlScope;
import uz.mpp.platformcontracts.common.v1.ExecutionControlState;
import uz.mpp.platformcontracts.grpc.v1.ApplyOverrideRequest;
import uz.mpp.platformcontracts.grpc.v1.ClearOverrideRequest;
import uz.mpp.platformcontracts.grpc.v1.ExecutionControlServiceGrpc;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * trigger_freeze / trigger_unfreeze (service_internal_methods.md §5.3) —
 * gRPC на Execution Control Service, scope=PARTNER_STAGE,
 * scope_id="{partner_id}:BILLING" (platform-contracts/grpc/internal_control.proto
 * докстринг: "Billing Reconciliation → Execution Control Service
 * (freeze/unfreeze партнёра)... scope=PARTNER_STAGE, stage=BILLING"). Реальный
 * gRPC-клиент, не проверялся против живого Execution Control Service в этой
 * песочнице.
 */
public final class ExecutionControlClient implements AutoCloseable {

    private static final String REQUESTED_BY = "billing-reconciliation";

    private final ManagedChannel channel;
    private final ExecutionControlServiceGrpc.ExecutionControlServiceBlockingStub stub;

    public ExecutionControlClient(String host, int port) {
        // ManagedChannelBuilder.forAddress(host, port) уходит через NameResolverRegistry
        // по scheme таргета — реальная находка в delivery-service/OperatorSubmitClient.java
        // (прогон против живого SMSC): в grpc-netty-shaded это иногда резолвится в
        // зарегистрированный UdsNameResolverProvider ("unix") вместо dns и падает даже
        // для обычного IP:port. forAddress(SocketAddress) обходит резолвер целиком.
        this.channel = NettyChannelBuilder.forAddress(new InetSocketAddress(host, port)).usePlaintext().build();
        this.stub = ExecutionControlServiceGrpc.newBlockingStub(channel);
    }

    ExecutionControlClient(ExecutionControlServiceGrpc.ExecutionControlServiceBlockingStub stub) {
        this.channel = null;
        this.stub = stub;
    }

    private static String scopeId(String partnerId) {
        return partnerId + ":BILLING";
    }

    /** trigger_freeze — PARTNER_STAGE=BILLING, PAUSED. */
    public long triggerFreeze(String partnerId, String reason) {
        var resp = stub.applyOverride(ApplyOverrideRequest.newBuilder()
            .setScope(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_PARTNER_STAGE)
            .setScopeId(scopeId(partnerId))
            .setState(ExecutionControlState.EXECUTION_CONTROL_STATE_PAUSED)
            .setAdmissionRate(0.0)
            .setReason(reason)
            .setRequestedBy(REQUESTED_BY)
            .build());
        return resp.getVersion();
    }

    /** trigger_unfreeze — снимает override для PARTNER_STAGE=BILLING, возврат к ACTIVE. */
    public long triggerUnfreeze(String partnerId) {
        var resp = stub.clearOverride(ClearOverrideRequest.newBuilder()
            .setScope(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_PARTNER_STAGE)
            .setScopeId(scopeId(partnerId))
            .setRequestedBy(REQUESTED_BY)
            .build());
        return resp.getVersion();
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