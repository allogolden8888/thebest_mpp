package uz.mpp.msr;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        HealthServer health = new HealthServer(9090);
        health.start();

        MessageStateStore store = new MessageStateStore();

        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092");
        // instance id — см. KafkaIo.buildTransactionalProducer javadoc: не
        // решает fencing под ребалансировкой для >1 реплики в этом срезе.
        String instanceId = System.getenv().getOrDefault("MSR_INSTANCE_ID", UUID.randomUUID().toString());
        KafkaConsumer<String, byte[]> consumer = KafkaIo.buildConsumer(bootstrapServers, "message-state-resolver");
        KafkaProducer<String, byte[]> producer = KafkaIo.buildTransactionalProducer(bootstrapServers, "message-state-resolver-" + instanceId);

        health.ready.set(true);

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            consumer.wakeup();
            health.stop();
        }, "message-state-resolver-shutdown"));

        try {
            KafkaIo.run(consumer, producer, store, running);
        } finally {
            consumer.close();
            producer.close();
        }
    }
}
