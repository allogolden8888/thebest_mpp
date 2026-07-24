package uz.mpp.billing;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String tariffPath = System.getenv().getOrDefault("BILLING_TARIFF_PATH",
            "../../config_schemas/examples/billing_tariff.valid.json");
        TariffResolver tariffResolver = TariffResolver.fromFile(Path.of(tariffPath));
        BillingService billingService = new BillingService(tariffResolver);

        HealthServer health = new HealthServer(9090);
        health.start();

        String redisUrl = System.getenv().getOrDefault("REDIS_BILLING_URL", "redis://redis-billing.mpp.svc:6379");
        BillingAccountStore accountStore = new BillingAccountStore(redisUrl);

        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092");
        KafkaConsumer<String, byte[]> consumer = KafkaIo.buildConsumer(bootstrapServers, "billing-service");
        KafkaProducer<String, byte[]> producer = KafkaIo.buildProducer(bootstrapServers);

        // readyz флипается true только после того, как все клиенты (Kafka/Redis)
        // сконструированы — кодревью: "/readyz флипается true до конструирования
        // Redis store/Kafka consumer/producer". Важная оговорка: Kafka/Lettuce
        // клиенты ленивые — "сконструирован" не значит "реально достижим", полная
        // readiness-проверка (например Redis PING) не реализована в этом срезе.
        health.ready.set(true);

        String accountId = System.getenv().getOrDefault("BILLING_ACCOUNT_ID", "test-partner");

        AtomicBoolean running = new AtomicBoolean(true);
        // KafkaConsumer не потокобезопасен — close() из другого потока, пока
        // основной поток внутри poll(), некорректен. wakeup() — единственный
        // потокобезопасный способ прервать блокирующий poll() извне; сам
        // KafkaIo.run() ловит WakeupException и закрывает consumer/producer
        // из СВОЕГО потока (см. KafkaIo.java).
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            consumer.wakeup();
            health.stop();
            accountStore.close();
        }, "billing-service-shutdown"));

        try {
            KafkaIo.run(consumer, producer, accountStore, billingService, accountId, running);
        } finally {
            consumer.close();
            producer.close();
        }
    }
}
