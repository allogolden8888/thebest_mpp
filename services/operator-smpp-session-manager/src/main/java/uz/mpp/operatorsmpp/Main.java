package uz.mpp.operatorsmpp;

import com.google.protobuf.Timestamp;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import uz.mpp.operatorsmpp.client.OperatorSmppClient;
import uz.mpp.operatorsmpp.core.PriorityGate;
import uz.mpp.operatorsmpp.core.TokenBucket;
import uz.mpp.operatorsmpp.grpcserver.OperatorQuerySmServer;
import uz.mpp.operatorsmpp.grpcserver.OperatorSubmitServer;
import uz.mpp.operatorsmpp.health.HealthServer;
import uz.mpp.operatorsmpp.kafkaio.OperatorEventPublisher;
import uz.mpp.operatorsmpp.registry.OperatorRouteRegistry;
import uz.mpp.platformcontracts.common.v1.Protocol;
import uz.mpp.platformcontracts.events.v1.OperatorDlr;

import java.time.Duration;
import java.time.Instant;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Operator SMPP Session Manager (services_specifictaion.md §2.3) — SMPP
 * binds с операторами, reconnect, enquire_link, TPS/throttling,
 * submit_sm/DLR, приоритет submit_sm над query_sm.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        HealthServer health = new HealthServer();
        health.start();

        String operatorId = env("OPERATOR_ID", "beeline_uz");
        String routeId = env("ROUTE_ID", "route-1");
        String kafkaBrokers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092");
        OperatorEventPublisher eventPublisher = new OperatorEventPublisher(kafkaBrokers);

        String redisUri = "redis://" + env("REDIS_RUNTIME_HOST", "localhost") + ":" + env("REDIS_RUNTIME_PORT", "6379");
        OperatorRouteRegistry routeRegistry = new OperatorRouteRegistry(redisUri, env("HOSTNAME", "operator-smpp-session-manager-0"), Duration.ofSeconds(30));

        OperatorSmppClient client = new OperatorSmppClient(dlrPdu -> {
            OperatorDlr dlr = OperatorDlr.newBuilder()
                .setOperatorId(operatorId)
                .setProtocol(Protocol.PROTOCOL_SMPP)
                .setRawStatus(new String(dlrPdu.shortMessage()))
                .setReceivedAt(toTimestamp(Instant.now()))
                .build();
            eventPublisher.publishDlr(dlr);
        });

        connectAndBindWithRetry(client, operatorId, routeId, routeRegistry);

        Timer enquireLinkTimer = new Timer("enquire-link-tick", true);
        enquireLinkTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (client.isActive()) {
                    client.sendEnquireLink();
                    routeRegistry.heartbeat(operatorId, routeId);
                }
            }
        }, 30_000, 30_000);

        TokenBucket tpsBucket = new TokenBucket(
            Double.parseDouble(env("TPS_LIMIT", "500")),
            Double.parseDouble(env("TPS_LIMIT", "500")),
            System.currentTimeMillis());
        PriorityGate priorityGate = new PriorityGate(Integer.parseInt(env("MAX_CONCURRENT_SUBMITS", "100")));

        Server grpcServer = ServerBuilder.forPort(Integer.parseInt(env("GRPC_PORT", "9000")))
            .addService(new OperatorSubmitServer(client, tpsBucket, priorityGate, eventPublisher))
            .addService(new OperatorQuerySmServer(priorityGate))
            .build()
            .start();
        System.out.println("gRPC OperatorSubmitService/OperatorQuerySmService слушает :" + env("GRPC_PORT", "9000"));

        health.setReady(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            enquireLinkTimer.cancel();
            grpcServer.shutdown();
            client.close();
            routeRegistry.unregister(operatorId, routeId);
            routeRegistry.close();
            eventPublisher.close();
            health.stop();
        }));

        grpcServer.awaitTermination();
    }

    /**
     * reconnect (service_internal_methods.md §1.3) — простая retry-петля с
     * фиксированной паузой. Реальная reconnect_policy (backoff, jitter,
     * лимит попыток) из partner config snapshot не подключена в этом срезе
     * (см. README "Что НЕ реализовано").
     */
    private static void connectAndBindWithRetry(OperatorSmppClient client, String operatorId, String routeId, OperatorRouteRegistry routeRegistry) throws InterruptedException {
        String host = env("OPERATOR_SMSC_HOST", "localhost");
        int port = Integer.parseInt(env("OPERATOR_SMSC_PORT", "2775"));
        String systemId = env("SMPP_SYSTEM_ID", "mpp_esme");
        String password = env("SMPP_PASSWORD", "demo_password");

        int attempt = 0;
        while (true) {
            try {
                client.connect(host, port);
                client.bind(systemId, password, "", 5000);
                long routeEpoch = System.nanoTime();
                routeRegistry.register(operatorId, routeId, routeEpoch, host + ":" + port);
                return;
            } catch (Exception e) {
                attempt++;
                System.err.println("bind_operator попытка " + attempt + " не удалась: " + e.getMessage());
                Thread.sleep(Math.min(30_000, 1000L * attempt));
            }
        }
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }
}