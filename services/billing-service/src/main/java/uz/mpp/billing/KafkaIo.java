package uz.mpp.billing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import uz.mpp.billing.BillingAccountState.Account;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Kafka I/O — потребляет {@code stage.billing}, публикует {@code stage.completed}
 * (platform_contracts.md). Тот же паттерн, что у Rust-сервисов этого среза:
 * {@link BillingService#handleBillingExecute} — чистая функция, тестируется
 * без сети; {@link #run} — реальный consume/produce цикл, не интеграционно
 * проверенный в этом окружении (нет живого Kafka-брокера).
 *
 * <p><b>account_id</b>: ни один proto-контракт не несёт явно, откуда берётся
 * ключ счёта в Billing Redis — здесь используется {@code partner_id}, тем же
 * способом, каким предполагается резолвиться тариф (см. {@code BillingService}
 * javadoc про открытую находку с источником {@code partner_id}). Для среза
 * Фазы 2.2 (один тестовый партнёр) зашит константой через
 * {@code BILLING_ACCOUNT_ID} env var.
 */
public final class KafkaIo {

    private static final Logger LOG = Logger.getLogger(KafkaIo.class.getName());
    public static final String INPUT_TOPIC = "stage.billing";
    public static final String OUTPUT_TOPIC = "stage.completed";

    public static KafkaConsumer<String, byte[]> buildConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    public static KafkaProducer<String, byte[]> buildProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void run(
        KafkaConsumer<String, byte[]> consumer,
        KafkaProducer<String, byte[]> producer,
        BillingAccountStore accountStore,
        BillingService billingService,
        String accountId
    ) {
        consumer.subscribe(List.of(INPUT_TOPIC));
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, byte[]> record : records) {
                try {
                    StageExecuteCommand command = StageExecuteCommand.parseFrom(record.value());
                    Account account = accountStore.fetch(accountId);
                    BillingService.Result result = billingService.handleBillingExecute(command, account, account.epoch());
                    accountStore.save(accountId, result.updatedAccount());

                    StageCompletedEvent event = result.event();
                    producer.send(new ProducerRecord<>(OUTPUT_TOPIC, event.getMessageId(), event.toByteArray())).get();
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "не удалось обработать stage.billing запись, offset не коммитится", e);
                    continue;
                }
                consumer.commitAsync();
            }
        }
    }
}
