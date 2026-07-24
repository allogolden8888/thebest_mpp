package uz.mpp.delivery;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import uz.mpp.platformcontracts.grpc.v1.OperatorSubmitServiceGrpc;
import uz.mpp.platformcontracts.grpc.v1.SubmitRequest;
import uz.mpp.platformcontracts.grpc.v1.SubmitResponse;

/**
 * {@code call_submit} (service_internal_methods.md §1.8) — реальный gRPC-клиент
 * против {@code OperatorSubmitService} (`platform-contracts/grpc/operator_gateway.proto`),
 * сгенерированный настоящим {@code protoc-gen-grpc-java}, не заглушка. Сервер
 * этого контракта (Operator SMPP Session Manager / Operator HTTP Gateway) не
 * реализован ни в одном репозитории на момент написания (владелец —
 * Субагент 1), поэтому вызов компилируется и типобезопасен, но live не
 * проверен — см. README.
 *
 * <p>{@link ManagedChannel} кэшируется по {@code endpoint} — не открывает
 * новое TCP/HTTP2-соединение на каждый submit (в отличие от {@link
 * MessageContextStore}/{@link GatewayRegistry}, где новое Redis-соединение на
 * вызов — задокументированный, тот же класс компромисса, что в
 * billing-service — здесь сделано правильно сразу, потому что gRPC-канал
 * по своей природе держит пул HTTP/2-соединений и рассчитан на переиспользование,
 * не на конструирование заново).
 */
public final class OperatorSubmitClient implements AutoCloseable {

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final long deadlineMillis;

    public OperatorSubmitClient(long deadlineMillis) {
        this.deadlineMillis = deadlineMillis;
    }

    private ManagedChannel channelFor(String endpoint) {
        return channels.computeIfAbsent(endpoint, e -> {
            String[] parts = e.split(":", 2);
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 443;
            return NettyChannelBuilder.forAddress(host, port).usePlaintext().build();
        });
    }

    public SubmitResponse submit(String endpoint, SubmitRequest request) {
        ManagedChannel channel = channelFor(endpoint);
        OperatorSubmitServiceGrpc.OperatorSubmitServiceBlockingStub stub =
            OperatorSubmitServiceGrpc.newBlockingStub(channel).withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS);
        return stub.submit(request);
    }

    @Override
    public void close() {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdownNow();
        }
    }
}
