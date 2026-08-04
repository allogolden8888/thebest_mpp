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

        // HIGH находка кодревью (PART 2, message-state-resolver #3): restore
        // ДО readyz=true и ДО подписки на живой трафик — иначе первые
        // stage.completed/delivery.status после рестарта пода видят пустой
        // store и трактуют реально известные message_id как "первый раз
        // видим", ломая REGRESSION-детекцию и lifecycle_version.
        KafkaIo.restoreFromChangelog(bootstrapServers, store);

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
