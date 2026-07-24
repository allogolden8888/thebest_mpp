package uz.mpp.billingreconciliation.grpcclient;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.ExecutionControlScope;
import uz.mpp.platformcontracts.common.v1.ExecutionControlState;
import uz.mpp.platformcontracts.grpc.v1.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Реальный gRPC round-trip через in-process транспорт (не мок бизнес-логики
 * клиента) — фейковый ExecutionControlService фиксирует, какой именно
 * ApplyOverrideRequest/ClearOverrideRequest реально отправил
 * {@link ExecutionControlClient}.
 */
class ExecutionControlClientTest {

    private Server server;
    private ManagedChannel channel;

    private static class CapturingExecutionControlService extends ExecutionControlServiceGrpc.ExecutionControlServiceImplBase {
        final CompletableFuture<ApplyOverrideRequest> applyRequest = new CompletableFuture<>();
        final CompletableFuture<ClearOverrideRequest> clearRequest = new CompletableFuture<>();

        @Override
        public void applyOverride(ApplyOverrideRequest request, StreamObserver<ApplyOverrideResponse> responseObserver) {
            applyRequest.complete(request);
            responseObserver.onNext(ApplyOverrideResponse.newBuilder().setVersion(1).build());
            responseObserver.onCompleted();
        }

        @Override
        public void clearOverride(ClearOverrideRequest request, StreamObserver<ApplyOverrideResponse> responseObserver) {
            clearRequest.complete(request);
            responseObserver.onNext(ApplyOverrideResponse.newBuilder().setVersion(2).build());
            responseObserver.onCompleted();
        }
    }

    private final CapturingExecutionControlService service = new CapturingExecutionControlService();

    private ExecutionControlServiceGrpc.ExecutionControlServiceBlockingStub startServerAndGetStub() throws Exception {
        String name = "test-" + System.nanoTime();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        return ExecutionControlServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (channel != null) channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        if (server != null) server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void triggerFreezeSendsPartnerStageBillingPaused() throws Exception {
        var stub = startServerAndGetStub();
        ExecutionControlClient client = newClientWithStub(stub);

        long version = client.triggerFreeze("acme", "drift severity HIGH");

        ApplyOverrideRequest sent = service.applyRequest.get(2, TimeUnit.SECONDS);
        assertEquals(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_PARTNER_STAGE, sent.getScope());
        assertEquals("acme:BILLING", sent.getScopeId());
        assertEquals(ExecutionControlState.EXECUTION_CONTROL_STATE_PAUSED, sent.getState());
        assertEquals(0.0, sent.getAdmissionRate());
        assertEquals(1, version);
    }

    @Test
    void triggerUnfreezeSendsPartnerStageBillingClear() throws Exception {
        var stub = startServerAndGetStub();
        ExecutionControlClient client = newClientWithStub(stub);

        long version = client.triggerUnfreeze("acme");

        ClearOverrideRequest sent = service.clearRequest.get(2, TimeUnit.SECONDS);
        assertEquals(ExecutionControlScope.EXECUTION_CONTROL_SCOPE_PARTNER_STAGE, sent.getScope());
        assertEquals("acme:BILLING", sent.getScopeId());
        assertEquals(2, version);
    }

    private static ExecutionControlClient newClientWithStub(ExecutionControlServiceGrpc.ExecutionControlServiceBlockingStub stub) throws Exception {
        var ctor = ExecutionControlClient.class.getDeclaredConstructor(ExecutionControlServiceGrpc.ExecutionControlServiceBlockingStub.class);
        ctor.setAccessible(true);
        return ctor.newInstance(stub);
    }
}