package uz.mpp.billing;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.nio.file.Path;

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
        health.ready.set(true);

        String redisUrl = System.getenv().getOrDefault("REDIS_BILLING_URL", "redis://redis-billing.mpp.svc:6379");
        BillingAccountStore accountStore = new BillingAccountStore(redisUrl);

        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092");
        KafkaConsumer<String, byte[]> consumer = KafkaIo.buildConsumer(bootstrapServers, "billing-service");
        KafkaProducer<String, byte[]> producer = KafkaIo.buildProducer(bootstrapServers);

        String accountId = System.getenv().getOrDefault("BILLING_ACCOUNT_ID", "test-partner");
        KafkaIo.run(consumer, producer, accountStore, billingService, accountId);
    }
}
