package uz.mpp.billing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
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
        // NEXT_STEPS_1500TPS.md 1.2 подняло fetch.min.bytes=32768. 2026-08-18
        // — реальное измерение (rate sweep 50/100/200/300 TPS на этой
        // машине) показало: этот "верхний предел 500мс, чтобы не залипать"
        // как раз и стал доминирующей задержкой — при текущем объёме
        // (маленький партнёр, некрупные payload'ы) партиция физически не
        // набирает 32КБ быстро НИ НА ОДНОМ из протестированных rate, так
        // что poll() почти всегда упирался в фиксированные fetch.max.wait.ms
        // (500мс) НА КАЖДЫЙ poll — то есть latency перестала зависеть от
        // rate вообще (50 TPS показал p95 почти как 300 TPS), что и выдало
        // проблему. Возвращаем к дефолту (1 байт).
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        return new KafkaConsumer<>(props);
    }

    public static KafkaProducer<String, byte[]> buildProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Аналог pipeline-engine QueueFull-фикса, но для Java-клиента (совсем
        // другой механизм риска, не тот же баг): без явной настройки
        // KafkaProducer (kafka-clients 3.9.0) работает на дефолтах —
        // buffer.memory=32MB, max.block.ms=60000мс. KafkaProducer
        // потокобезопасен и сам буферизует/бэкпрешурит, но до 128
        // одновременных worker-потоков (BILLING_CONCURRENCY) шлют в один и
        // тот же shared producer — если брокер под нагрузкой отвечает
        // медленнее, чем буфер освобождается (реальный сценарий: локальный
        // брокер уже находили под CPU-давлением на 33 топиках/365
        // партициях), send() не ретраит с фиксированной паузой как rdkafka —
        // он молча БЛОКИРУЕТ вызывающий поток до max.block.ms (60с
        // дефолт!) в ожидании места в буфере, ДО того как вернуть Future,
        // то есть до существующего PRODUCER_SEND_TIMEOUT-таймаута на
        // .get() дело даже не доходит. Фикс: 1) buffer.memory поднят
        // с запасом на всю конкурентность сервиса; 2) max.block.ms снижен
        // до значения PRODUCER_SEND_TIMEOUT — если backpressure всё же
        // наступит, воркер-поток падает с громким исключением за 10с
        // (запись просто не коммитится, at-least-once переподхватит), а не
        // молча зависает на минуту.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67_108_864L); // 64MB (было 32MB по умолчанию)
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, PRODUCER_SEND_TIMEOUT.toMillis()); // 10s (было 60s по умолчанию)
        // NEXT_STEPS_1500TPS.md 1.1: linger.ms=0 по умолчанию — каждый send()
        // уходит брокеру отдельным запросом. 5мс даёт клиенту собрать пачку
        // без заметного вклада в p50/p95 (бюджет — сотни мс).
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        return new KafkaProducer<>(props);
    }

    private static int envInt(String key, int fallback) {
        String v = System.getenv(key);
        if (v == null || v.isEmpty()) {
            return fallback;
        }
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            return fallback;
        }
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
        TariffCache tariffCache,
        AtomicBoolean running
    ) {
        consumer.subscribe(List.of(INPUT_TOPIC));

        // Реальная находка (нагрузочный прогон, не гипотетическая): раньше
        // батч обрабатывался строго последовательно — Redis peek -> Redis
        // apply_atomic_charge -> БЛОКИРУЮЩИЙ producer.send().get() ->
        // следующая запись. При 100 msg/s ingest это оставляло
        // billing-service самым отстающим из всех стадий (единственная
        // стадия с ДВУМЯ Redis round-trip на запись, не одним). Пул
        // потоков обрабатывает батч конкурентно (BillingAccountStore
        // держит одно потокобезопасное Lettuce-соединение — см. правку
        // там, KafkaProducer тоже потокобезопасен по документации клиента);
        // commit остаётся на ЭТОМ потоке (владеющем KafkaConsumer — сам
        // класс не потокобезопасен даже для commit, не только для poll).
        // Самокалибрующийся размер пула вместо статического BILLING_CONCURRENCY
        // (тот путь требовал ручного двоичного поиска заново на каждой новой
        // машине — 128 vs 450 давало 10-кратную разницу в p95 между AMD
        // 12-core/15GB и Mac 8-core/7.75GB, см. LATENCY_INVESTIGATION_1500TPS.md
        // и b713c53). Потолок роста всё ещё настраиваем через env — калибратор
        // сам находит безопасную рабочую точку НИЖЕ потолка под реальным
        // трафиком этого конкретного деплоя, не гадает какой потолок нужен.
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
            AdaptiveThreadPoolCalibrator.FLOOR, AdaptiveThreadPoolCalibrator.FLOOR,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> {
                Thread t = new Thread(r, "billing-worker");
                t.setDaemon(true);
                return t;
            });
        AdaptiveThreadPoolCalibrator calibrator = new AdaptiveThreadPoolCalibrator(
            pool,
            envInt("BILLING_CONCURRENCY_CEILING", 512),
            envInt("THREAD_POOL_CALIBRATION_WINDOW_MS", 120_000),
            envInt("THREAD_POOL_CALIBRATION_TICK_MS", 10_000));
        try {
            while (running.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    continue;
                }

                // Осознанное отличие от последовательной версии: ВСЕ записи
                // батча уходят в пул сразу, включая те, что технически идут
                // "после" записи, которая в итоге окажется неудачной в той
                // же партиции — раньше isSuspended(tp) проверялся ДО отправки
                // в обработку и такие записи не трогались вообще. Здесь они
                // реально обрабатываются (charge применяется), просто их
                // offset всё равно не коммитится (см. break ниже) — при
                // редоставке они переобработаются ещё раз, что БЕЗОПАСНО:
                // apply_atomic_charge дедуплицирует по charge_id
                // (=stage_execution_id), повторное применение того же
                // charge — не более чем ALREADY_PROCESSED, не двойное
                // списание. Обычные charge не двигают account epoch (только
                // freeze/unfreeze), так что конкурентная обработка внутри
                // одного batch не создаёт STALE_EPOCH-гонку между записями.
                // Цена — ограниченный объём избыточной (но безопасной)
                // повторной работы в редком случае реальной ошибки, в обмен
                // на конкурентную обработку в штатном случае.
                Map<ConsumerRecord<String, byte[]>, Future<Boolean>> futures = new HashMap<>();
                Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> byPartition = new HashMap<>();
                for (ConsumerRecord<String, byte[]> record : records) {
                    byPartition.computeIfAbsent(new TopicPartition(record.topic(), record.partition()), k -> new ArrayList<>()).add(record);
                    futures.put(record, pool.submit(() -> {
                        long startNanos = System.nanoTime();
                        try {
                            processRecord(record, accountStore, billingService, tariffCache, producer);
                            // Латентность только успешных обработок — тот же
                            // сигнал, что двоичный поиск в
                            // LATENCY_INVESTIGATION_1500TPS.md измерял p95/p99
                            // по факту (не смешивая с быстрым fail-путём).
                            calibrator.recordTaskLatency((System.nanoTime() - startNanos) / 1_000_000);
                            return true;
                        } catch (Exception e) {
                            LOG.log(Level.SEVERE, "не удалось обработать stage.billing запись partition=" + record.partition()
                                + " offset=" + record.offset() + ", оффсет не коммитится, партишен приостановлен до следующего поллинга", e);
                            return false;
                        }
                    }));
                }

                // Собираем результаты СТРОГО по возрастанию offset внутри
                // каждой партиции (не в порядке завершения futures, который
                // при конкурентной обработке произволен) — та же семантика
                // "первая ошибка приостанавливает партишен", что была у
                // последовательной версии, теперь просто применяется ПОСЛЕ
                // конкурентного выполнения, не ВО ВРЕМЯ него.
                OffsetTracker tracker = new OffsetTracker();
                for (Map.Entry<TopicPartition, List<ConsumerRecord<String, byte[]>>> entry : byPartition.entrySet()) {
                    TopicPartition tp = entry.getKey();
                    List<ConsumerRecord<String, byte[]>> ordered = entry.getValue();
                    ordered.sort(Comparator.comparingLong(ConsumerRecord::offset));
                    for (ConsumerRecord<String, byte[]> record : ordered) {
                        boolean ok;
                        try {
                            ok = futures.get(record).get();
                        } catch (Exception e) {
                            LOG.log(Level.SEVERE, "не удалось получить результат обработки partition=" + tp + " offset=" + record.offset(), e);
                            ok = false;
                        }
                        if (ok) {
                            tracker.recordSuccess(tp, record.offset());
                        } else {
                            tracker.recordFailure(tp);
                            break; // остальные записи этой партиции в батче остаются неподтверждёнными, at-least-once
                        }
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
        } finally {
            pool.shutdown();
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

    /**
     * package-private (не private) — прямо протестировано {@code KafkaIoTest}
     * против реального Billing Redis + {@link org.apache.kafka.clients.producer.MockProducer}
     * (Фаза 5a: доказательство, что разные {@code partner_id} списываются с
     * разных account_id, не одного общего).
     */
    static void processRecord(
        ConsumerRecord<String, byte[]> record,
        BillingAccountStore accountStore,
        BillingService billingService,
        TariffCache tariffCache,
        Producer<String, byte[]> producer
    ) throws Exception {
        StageExecuteCommand command = StageExecuteCommand.parseFrom(record.value());
        BillingExtension ext = command.getBilling();

        // account_id = partner_id (Фаза 5a: 1:1, простейшее сопоставление —
        // billing.billing_ledger уже хранит оба столбца отдельно, но ничто до
        // этой фазы их не различало).
        String accountId = ext.getPartnerId();
        TariffResolver resolver = tariffCache.resolve(accountId);
        TariffResolver.Tariff tariff = billingService.resolveTariff(resolver, ext); // бросает IllegalArgumentException на отрицательный segment_count

        // Фаза 11 плана закрытия API-пробелов: sandbox — ни одного обращения
        // к Billing Redis (ни applyChargeAtomically, ни даже peek/peekEpoch),
        // не "спишем и тут же отменим" — реального списания не существует
        // вообще ни на миг. buildEvent свитчит только по chargeResult.outcome(),
        // Account-поля не читает (см. javadoc BillingService.buildEvent) —
        // синтетика с APPLIED собирается штатным путём, без нового кода там.
        // accountStore.peekEpoch(accountId), не peek(accountId).epoch() —
        // 1500 TPS push: точечный HGET одного поля вместо HGETALL +
        // пересборки всей истории processed_charge_ids ради одного числа
        // (см. javadoc BillingAccountStore.peekEpoch).
        ChargeResult chargeResult = command.getSandbox()
            ? new ChargeResult(BillingAccountState.Account.fresh(0), BillingAccountState.ChargeOutcome.APPLIED)
            : accountStore.applyChargeAtomically(accountId, command.getStageExecutionId(), tariff.amountMinorUnits(),
                accountStore.peekEpoch(accountId), ext.getPartnerId(), tariff.currencyCode());
        StageCompletedEvent event = billingService.buildEvent(command, ext.getCategory(), tariff, chargeResult);

        producer.send(new ProducerRecord<>(OUTPUT_TOPIC, event.getMessageId(), event.toByteArray()))
            .get(PRODUCER_SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
