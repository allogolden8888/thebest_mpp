package uz.mpp.partnersmpp;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import uz.mpp.partnersmpp.grpcserver.DeliverSmServer;
import uz.mpp.partnersmpp.health.HealthServer;
import uz.mpp.partnersmpp.kafkaio.IncomingPublisher;
import uz.mpp.partnersmpp.registry.SessionRedisRegistry;
import uz.mpp.partnersmpp.server.ChannelRegistry;
import uz.mpp.partnersmpp.server.PartnerSmppServer;
import uz.mpp.partnersmpp.server.StaticAuthenticator;

import java.time.Duration;
import java.util.Map;

/**
 * Partner SMPP Gateway (services_specifictaion.md §2.2) — SMPP bind/unbind,
 * submit_sm, deliver_sm, partner-side query_sm, enquire_link.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        HealthServer health = new HealthServer();
        health.start();

        String kafkaBrokers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092");
        IncomingPublisher publisher = new IncomingPublisher(kafkaBrokers);

        String redisUri = "redis://" + env("REDIS_RUNTIME_HOST", "localhost") + ":" + env("REDIS_RUNTIME_PORT", "6379");
        SessionRedisRegistry sessionRegistry = new SessionRedisRegistry(redisUri, env("HOSTNAME", "partner-smpp-gateway-0"), Duration.ofSeconds(30));

        // Партнёрские credentials — заглушка на один статический system_id
        // (реальный источник: partner config snapshot из config.changes,
        // см. README "Что НЕ реализовано" — не подключено в этом срезе).
        StaticAuthenticator authenticator = new StaticAuthenticator(Map.of(
            env("SMPP_SYSTEM_ID", "demo_system_id"),
            new StaticAuthenticator.Credential(
                env("SMPP_PASSWORD", "demo_password"),
                env("SMPP_PARTNER_ID", "demo_partner"),
                env("SMPP_APPLICATION_ID", "demo_application")
            )
        ));

        ChannelRegistry channelRegistry = new ChannelRegistry();
        String endpoint = env("HOSTNAME", "partner-smpp-gateway-0") + ":" + env("SMPP_PORT", "2775");
        PartnerSmppServer smppServer = new PartnerSmppServer(
            authenticator,
            publisher::publish,
            Double.parseDouble(env("RATE_LIMIT_TPS", "300")),
            channelRegistry,
            (partnerId, systemId, sessionEpoch) ->
                sessionRegistry.register(partnerId, systemId, systemId + "-" + sessionEpoch, sessionEpoch, endpoint),
            sessionRegistry::unregister
        );
        int smppPort = smppServer.start(Integer.parseInt(env("SMPP_PORT", "2775")));
        System.out.println("Partner SMPP Gateway слушает :" + smppPort);

        Server grpcServer = ServerBuilder.forPort(Integer.parseInt(env("GRPC_PORT", "9000")))
            .addService(new DeliverSmServer(channelRegistry))
            .build()
            .start();
        System.out.println("gRPC PartnerDeliverSmService слушает :" + Integer.parseInt(env("GRPC_PORT", "9000")));

        health.setReady(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            grpcServer.shutdown();
            smppServer.stop();
            sessionRegistry.close();
            publisher.close();
            health.stop();
        }));

        grpcServer.awaitTermination();
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }
}
