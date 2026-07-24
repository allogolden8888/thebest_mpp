package uz.mpp.billing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import uz.mpp.billing.BillingAccountState.ChargeResult;
import uz.mpp.platformcontracts.common.v1.BillingExtension;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Kafka I/O — потребляет {@code stage.billing}, публикует {@code stage.completed}.
 *
 * <p><b>Кодревью нашло реальный баг здесь, исправлено, не только
 * задокументировано:</b> первая версия коммитила {@code consumer.commitAsync()}
 * без аргументов после каждой удачной записи — этот вызов коммитит **текущую
 * позицию всего фетча**, не "оффсет только что обработанной записи". Если
 * запись B из батча {@code [A, B, C]} падает и перехватывается
 * {@code try/catch { continue; }}, а C после неё обрабатывается успешно и
 * коммитит позицию ПОСЛЕ C — B навсегда потеряна: её оффсет уже позади
 * закоммиченной позиции, при рестарте она не переобработается. Никакого
 * списания, никакого {@code stage.completed}, Pipeline Engine ждёт этот
 * {@code stage_execution_id} бесконечно.
 *
 * <p>Исправление — коммит по каждому {@code TopicPartition} отдельно, только
 * до первой ошибки в этом партишене за этот поллинг (не после неё) —
 * {@link #run} ниже отслеживает {@code committableOffset}/{@code partitionsWithFailure}
 * по партишену, не одну глобальную позицию.
 */
public final class KafkaIo {

    private static final Logger LOG = Logger.getLogger(KafkaIo.class.getName());
    public static final String INPUT_TOPIC = "stage.billing";
    public static final String OUTPUT_TOPIC = "stage.completed";
    private static final Duration PRODUCER_SEND_TIMEOUT = Duration.ofSeconds(10);

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

    /**
     * @param running volatile флаг для graceful shutdown — main.java выставляет false
     *                в shutdown hook, цикл проверяет его между поллингами, не {@code while(true)}
     *                без выхода (кодревью: "no graceful shutdown anywhere").
     */
    public static void run(
        KafkaConsumer<String, byte[]> consumer,
        KafkaProducer<String, byte[]> producer,
        BillingAccountStore accountStore,
        BillingService billingService,
        String accountId,
        AtomicBoolean running
    ) {
        consumer.subscribe(List.of(INPUT_TOPIC));
        try {
            while (running.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));

                OffsetTracker tracker = new OffsetTracker();
                for (ConsumerRecord<String, byte[]> record : records) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    if (tracker.isSuspended(tp)) {
                        // Уже была ошибка раньше в этом партишене за этот поллинг — не
                        // обрабатываем и не коммитим дальше, чтобы не перепрыгнуть через
                        // непереработанную запись (ровно баг, который здесь исправлен).
                        continue;
                    }
                    try {
                        processRecord(record, accountStore, billingService, accountId, producer);
                        tracker.recordSuccess(tp, record.offset());
                    } catch (Exception e) {
                        LOG.log(Level.SEVERE, "не удалось обработать stage.billing запись partition=" + tp
                            + " offset=" + record.offset() + ", оффсет не коммитится, партишен приостановлен до следующего поллинга", e);
                        tracker.recordFailure(tp);
                    }
                }

                Map<TopicPartition, Long> committable = tracker.committableOffsets();
                if (!committable.isEmpty()) {
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    committable.forEach((tp, offset) -> offsets.put(tp, new OffsetAndMetadata(offset)));
                    consumer.commitSync(offsets);
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            if (running.get()) {
                throw e; // wakeup() неожиданный (не от shutdown hook) — не глотать
            }
            // иначе — штатный сигнал остановки от shutdown hook, выходим тихо.
        }
    }

    /**
     * Чистая, юнит-тестируемая логика "что коммитить" — вынесена из {@link #run}
     * специально, потому что кодревью отметило: сам баг (#1) был **полностью
     * непокрыт тестами**, так как жил внутри метода, требующего живого
     * {@link KafkaConsumer}. Здесь — та же логика без единой Kafka-зависимости.
     */
    static final class OffsetTracker {
        private final Map<TopicPartition, Long> committable = new HashMap<>();
        private final Set<TopicPartition> suspended = new HashSet<>();

        boolean isSuspended(TopicPartition tp) {
            return suspended.contains(tp);
        }

        void recordSuccess(TopicPartition tp, long offset) {
            committable.put(tp, offset + 1);
        }

        void recordFailure(TopicPartition tp) {
            suspended.add(tp);
        }

        Map<TopicPartition, Long> committableOffsets() {
            return committable;
        }
    }

    private static void processRecord(
        ConsumerRecord<String, byte[]> record,
        BillingAccountStore accountStore,
        BillingService billingService,
        String accountId,
        KafkaProducer<String, byte[]> producer
    ) throws Exception {
        StageExecuteCommand command = StageExecuteCommand.parseFrom(record.value());
        BillingExtension ext = command.getBilling();

        TariffResolver.Tariff tariff = billingService.resolveTariff(ext); // бросает IllegalArgumentException на отрицательный segment_count
        ChargeResult chargeResult = accountStore.applyChargeAtomically(accountId, command.getStageExecutionId(), tariff.amountMinorUnits(), accountStore.peek(accountId).epoch());
        StageCompletedEvent event = billingService.buildEvent(command, ext.getCategory(), tariff, chargeResult);

        producer.send(new ProducerRecord<>(OUTPUT_TOPIC, event.getMessageId(), event.toByteArray()))
            .get(PRODUCER_SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
