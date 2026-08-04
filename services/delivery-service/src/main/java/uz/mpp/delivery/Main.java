package uz.mpp.delivery;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        HealthServer health = new HealthServer(9090);
        health.start();

        String redisRuntimeUrl = RedisUrl.buildRuntimeUrl();
        MessageContextStore contextStore = new MessageContextStore(redisRuntimeUrl);
        GatewayRegistry gatewayRegistry = new GatewayRegistry(redisRuntimeUrl);
        ControlSnapshot controlSnapshot = new ControlSnapshot();
        OperatorSubmitClient submitClient = new OperatorSubmitClient(5000);
        SubmitIdempotencyStore idempotencyStore = new SubmitIdempotencyStore(redisRuntimeUrl);

        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092");
        KafkaConsumer<String, byte[]> consumer = KafkaIo.buildConsumer(bootstrapServers, "delivery-service");
        KafkaProducer<String, byte[]> producer = KafkaIo.buildProducer(bootstrapServers);

        // readyz — только после конструирования всех клиентов, тот же порядок,
        // что уже исправлен по кодревью в billing-service (см. Main.java там).
        health.ready.set(true);

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            consumer.wakeup();
            health.stop();
            contextStore.close();
            gatewayRegistry.close();
            submitClient.close();
            idempotencyStore.close();
        }, "delivery-service-shutdown"));

        try {
            KafkaIo.run(consumer, producer, contextStore, gatewayRegistry, controlSnapshot, submitClient, idempotencyStore, running);
        } finally {
            consumer.close();
            producer.close();
        }
    }
}
