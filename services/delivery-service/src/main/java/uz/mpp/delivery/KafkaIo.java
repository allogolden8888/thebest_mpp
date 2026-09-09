package uz.mpp.delivery;

import io.lettuce.core.RedisFuture;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import uz.mpp.delivery.DeliveryService.SubmitOutcome;
import uz.mpp.delivery.GatewayRegistry.GatewayEndpoint;
import uz.mpp.delivery.MessageContextStore.MessageContext;
import uz.mpp.delivery.SegmentMessage.Segment;
import uz.mpp.platformcontracts.common.v1.DeliveryExtension;
import uz.mpp.platformcontracts.common.v1.Outcome;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;
import uz.mpp.platformcontracts.events.v1.DeliveryStatusEvent;
import uz.mpp.platformcontracts.grpc.v1.SubmitRequest;
import uz.mpp.platformcontracts.grpc.v1.SubmitResponse;

/**
 * Kafka I/O — потребляет {@code stage.delivery}, публикует {@code stage.completed}.
 *
 * <p><b>Цикл опроса — непрерывный, без батч-барьера.</b> Раньше здесь был
 * классический «batch-and-wait»: {@code poll(N)} -> раздать весь батч в пул
 * -> дождаться КАЖДОГО future -> {@code commitSync()} -> только потом
 * следующий {@code poll()}. Это head-of-line blocking в чистом виде:
 * латентность записи равна её позиции в дренаже батча, а не времени её
 * обработки. Замер 300 TPS (8 CPU / 7.75GB) давал DELIVERY-хоп p50=1420мс
 * при ПУСТЫХ очередях ниже по потоку (окно SMPP 0-12 из 100, очередь
 * пейсера 0, диспатч 315/с ровно по входящему потоку) — то есть задержка
 * была чисто батчевой, мощности хватало.
 *
 * <p><b>Зафиксированный негативный результат (важно не повторить).</b>
 * Дважды пробовали лечить это уменьшением {@code max.poll.records} с 256 до
 * 64 — оба раза стало СИЛЬНО хуже: p99 617–1192мс -> 2860–5162мс. Вывод
 * «маленькие батчи вредны» из этого не следует: в тех прогонах оставался
 * {@code commitSync()} на КАЖДЫЙ батч, и при вчетверо более частых батчах
 * доминировать начинал именно синхронный round-trip коммита. Тот
 * эксперимент опровергал не идею «убрать барьер», а конкретный способ —
 * менять размер пачки, не трогая барьер и синхронный коммит. Поэтому
 * {@code KAFKA_MAX_POLL_RECORDS} остаётся 256: при непрерывном опросе
 * размер пачки вообще перестаёт определять латентность.
 *
 * <p><b>Как устроен новый цикл.</b> Итерация потока опроса:
 * (1) применить завершения из concurrent-очереди в
 * {@link OffsetWatermarkTracker}; (2) по счётчику in-flight решить
 * {@code pause()}/{@code resume()} на назначенных партициях; (3)
 * {@code poll()} с коротким таймаутом; (4) зарегистрировать оффсеты
 * пришедших записей и раздать их в пул, НЕ дожидаясь ничего; (5) раз в
 * {@code KAFKA_COMMIT_INTERVAL_MS} — {@code commitAsync} по watermark'у.
 * Опрос никогда не ждёт обработку; backpressure держится не отказом от
 * {@code poll()} (это уронило бы группу по {@code max.poll.interval.ms}), а
 * штатным {@code pause()}/{@code resume()}.
 *
 * <p><b>Гарантия коммита не ослаблена.</b> Оффсет считается завершённым
 * только когда получены ОБА подтверждения — Kafka-ack публикации
 * {@code stage.completed} и запись исхода в Redis (см. {@link PendingAcks}).
 * Ожидание неблокирующее: оба подтверждения — {@link CompletableFuture},
 * их {@code whenComplete} кладёт завершение в очередь. Коммитится только
 * НЕПРЕРЫВНЫЙ префикс завершённых оффсетов: если 12 готов, а 10 ещё в
 * работе, коммитится позиция 10, а не 13.
 *
 * <p><b>Один поток на consumer.</b> {@link KafkaConsumer} не
 * потокобезопасен, поэтому {@code poll}, {@code pause}/{@code resume} и
 * коммит выполняются исключительно в потоке опроса; воркеры не касаются
 * консьюмера вообще, они только пишут в {@link ConcurrentLinkedQueue}.
 *
 * <p>Осознанно не входит в этот срез (тот же явно раскрытый класс gap, что
 * {@code destination-resolution-service/kafka_io.rs}): DLQ на не парсящийся
 * {@code StageExecuteCommand}. {@code stage.delivery.dlq} уже запланирован в
 * {@code infra/kafka/}, но producer сюда не подключён; по архитектуре
 * (hld.md §20, "Critical Sweep") маршрутизация в DLQ по deadline — не
 * локальная забота отдельного stage-consumer'а. До её появления такая
 * запись обрабатывается как poison pill, см. {@link Dispatcher#fail}.
 */
public final class KafkaIo {

    private static final Logger LOG = Logger.getLogger(KafkaIo.class.getName());
    public static final String INPUT_TOPIC = "stage.delivery";
    public static final String OUTPUT_TOPIC = "stage.completed";
    // Фаза 11 плана закрытия API-пробелов: тот же топик, что использует
    // dlr-manager для настоящих DLR (см. services/dlr-manager/internal/kafkaio,
    // TopicDeliveryStatus) — message-state-resolver не отличает синтетику
    // от реального DLR, читает оба одинаково.
    public static final String DELIVERY_STATUS_TOPIC = "delivery.status";
    private static final Duration PRODUCER_SEND_TIMEOUT = Duration.ofSeconds(10);

    // Короткий таймаут опроса — не «сколько ждать данных», а «как быстро
    // цикл среагирует на завершения, на потолок in-flight и на таймер
    // коммита». Данные при непрерывном опросе и так приходят сразу
    // (fetch.min.bytes=1). 100мс на простое — 10 пустых poll'ов в секунду,
    // цена пренебрежимо мала против отзывчивости resume().
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    // Потолок ожидания обоих подтверждений (Kafka-ack + Redis). Без него
    // «зависшее» подтверждение держало бы watermark партиции навсегда:
    // одна запись без ack — и коммит не двигается, хотя тысячи следующих
    // готовы. По истечении запись идёт в общий путь ошибки (ретрай, затем
    // poison-pill), повторный submit при этом исключён идемпотентным store.
    private static final Duration ACK_TIMEOUT = Duration.ofSeconds(30);
    private static final int DEFAULT_MAX_IN_FLIGHT = 1024;
    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final long RETRY_BACKOFF_MS = 100;
    private static final Duration SHUTDOWN_DRAIN_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration FINAL_COMMIT_TIMEOUT = Duration.ofSeconds(10);

    public static KafkaConsumer<String, byte[]> buildConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // NEXT_STEPS_1500TPS.md 1.2 подняло fetch.min.bytes=32768, рассчитывая
        // сэкономить round trip'ы под высоким throughput. 2026-08-18 —
        // реальное измерение (rate sweep 50/100/200/300 TPS на этой машине,
        // 8 CPU/7.75GB) показало latency НЕ падает на низких rate — p95 на
        // 50 TPS (1760мс) почти как на 300 TPS — то есть узкое место не в
        // пропускной способности, а в фиксированной стоимости на хоп. При
        // текущем объёме сообщений партиция физически не набирает 32КБ
        // быстро, и консьюмер почти всегда упирается в fetch.max.wait.ms
        // (дефолт 500мс) — то есть эта "оптимизация" добавляла фиксированный
        // налог ~500мс НА ХОП. Оставлено на дефолте (1 байт).
        //
        // Про размер пачки. Пока цикл был «batch-and-wait», 256 было
        // компромиссом между длиной дренажа и частотой commitSync. Сейчас
        // барьера нет: записи раздаются в пул сразу и коммит идёт по
        // таймеру, поэтому размер пачки больше НЕ влияет на латентность
        // отдельной записи — он влияет только на размер одного фетча.
        // Дважды измеренная попытка снизить его до 64 при СТАРОМ цикле дала
        // ухудшение p99 с 617–1192мс до 2860–5162мс (чаще батчи -> чаще
        // синхронный коммит, который и стал доминировать) — см. javadoc
        // класса. Поэтому здесь сознательно оставлено 256: менять эту ручку
        // ради латентности бессмысленно, а память/GC на фетч она трогает.
        //
        // Настраиваемо через env, чтобы подбирать под железо БЕЗ пересборки
        // образа — тот же мотив, что у DELIVERY_CONCURRENCY рядом.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, envInt("KAFKA_MAX_POLL_RECORDS", 256));
        return new KafkaConsumer<>(props);
    }

    public static KafkaProducer<String, byte[]> buildProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Тот же класс риска, что уже найден и исправлен в
        // billing-service/KafkaIo.java#buildProducer (см. комментарий там
        // для полного разбора): без явной настройки KafkaProducer
        // (kafka-clients 3.9.0) работает на дефолтах — buffer.memory=32MB,
        // max.block.ms=60000мс — а до DELIVERY_CONCURRENCY (128 по
        // умолчанию) worker-потоков шлют в один и тот же shared producer.
        // Здесь риск даже более прямой, чем в billing: producer.send()
        // вызывается ПОСЛЕ блокирующего gRPC submit (реальный SMPP
        // round-trip), так что каждый воркер держит слот в буфере дольше,
        // прежде чем его сообщение реально уйдёт в сеть — если брокер
        // отстаёт от темпа продьюса, send() молча БЛОКИРУЕТ вызывающий
        // поток до max.block.ms (60с!) в ожидании места в буфере, ДО
        // возврата Future. Применяем тот же фикс превентивно:
        // 1) buffer.memory поднят с запасом на всю конкурентность сервиса;
        // 2) max.block.ms снижен до PRODUCER_SEND_TIMEOUT — если
        // backpressure всё же наступит, воркер падает громко за 10с
        // (at-least-once переподхватит), а не зависает на минуту молча.
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

    /** Завершение одной записи, положенное воркером в очередь для потока опроса. */
    record Completion(long epoch, TopicPartition partition, long offset, String claimedStageExecutionId) {
    }

    /**
     * Освобождение ключей идемпотентности {@code dlvsubmit:{stage_execution_id}}
     * после ПОДТВЕРЖДЁННОГО брокером коммита оффсета.
     *
     * <p><b>Зачем.</b> Раньше ключ жил ровно до 24-часового TTL, хотя нужен
     * он только пока запись может передоставиться. Замерено: Runtime Redis
     * рос до 1.16ГБ, а нехватка памяти на этой машине бьёт по хвостам
     * латентности напрямую. Требование сформулировано явно: состояние
     * терминального сообщения не должно оставаться ни в Redis, ни в Kafka.
     *
     * <p><b>Почему именно здесь, а не в {@code finalize.lua} pipeline-engine.</b>
     * Вариант «удалять ключ там же, где освобождается {@code msgctx}»
     * рассмотрен и отвергнут как источник ДУБЛЯ SMS абоненту.
     * {@code finalize} срабатывает через миллисекунды после публикации
     * {@code stage.completed}, то есть практически всегда РАНЬШЕ, чем
     * delivery-service закоммитит свой оффсет. Любая ошибка до коммита
     * приводит к передоставке записи; без ключа {@code claim} вернёт
     * {@code Won} и оператору уйдёт ВТОРОЕ сообщение — ровно то, ради
     * предотвращения чего {@link SubmitIdempotencyStore} и написан. Плюс тот
     * вариант покрывал бы только happy path: {@code stage_execution_id}
     * минтится заново на каждый диспатч (pipeline-engine/kafka_io.rs), и
     * ключи от retry-попыток остались бы висеть на TTL.
     *
     * <p><b>Почему здесь безопасно.</b> После подтверждённого коммита запись
     * не будет передоставлена этой consumer-группе, а повторная попытка
     * получит НОВЫЙ {@code stage_execution_id} и новый ключ. Старый ключ
     * мёртв по построению, а не по нашей оценке вероятности.
     *
     * <p>Освобождаем только в колбэке успешного {@code commitAsync}, а не по
     * факту его отправки: {@link #commitProgressAsync} помечает watermark
     * отправленным оптимистично (следующий коммит перекроет неудачный), и
     * освобождать по этой оптимистичной отметке значило бы вернуть тот же
     * дубль в сценарии «коммит не долетел + процесс упал».
     *
     * <p>Доступ однопоточный: заполняется в {@link #applyCompletions}, а
     * колбэки {@code commitAsync} Kafka вызывает в потоке опроса — том же
     * самом. Синхронизация не нужна и намеренно не добавлена.
     */
    static final class ClaimReleaser {

        private final Map<TopicPartition, java.util.NavigableMap<Long, String>> pending = new HashMap<>();
        private final SubmitIdempotencyStore store;

        ClaimReleaser(SubmitIdempotencyStore store) {
            this.store = store;
        }

        void track(TopicPartition partition, long offset, String stageExecutionId) {
            if (stageExecutionId == null) {
                return; // HOLD / sandbox / ранний REJECTED — claim не брался
            }
            pending.computeIfAbsent(partition, p -> new java.util.TreeMap<>()).put(offset, stageExecutionId);
        }

        /**
         * @param committed позиция «следующего к вычитыванию» оффсета — то,
         *        что реально подтвердил брокер. Освобождаются ключи записей
         *        строго ДО неё.
         */
        List<String> collectReleasable(Map<TopicPartition, Long> committed) {
            List<String> ids = new ArrayList<>();
            committed.forEach((partition, nextOffset) -> {
                java.util.NavigableMap<Long, String> byOffset = pending.get(partition);
                if (byOffset == null) {
                    return;
                }
                java.util.NavigableMap<Long, String> done = byOffset.headMap(nextOffset, false);
                ids.addAll(done.values());
                done.clear(); // headMap — вид, clear() удаляет из исходной карты
                if (byOffset.isEmpty()) {
                    pending.remove(partition);
                }
            });
            return ids;
        }

        void releaseCommitted(Map<TopicPartition, Long> committed) {
            List<String> ids = collectReleasable(committed);
            if (ids.isEmpty()) {
                return;
            }
            store.releaseAsync(ids); // без блокирующего ожидания — горячий путь
        }

        /** Партиция ушла при ребалансе: её записи передоставятся другому, ключи обязаны остаться. */
        void forget(Collection<TopicPartition> partitions) {
            partitions.forEach(pending::remove);
        }

        int pendingCount() {
            return pending.values().stream().mapToInt(Map::size).sum();
        }
    }

    /**
     * Решение о {@code pause()}/{@code resume()} по числу in-flight записей.
     * Вынесено отдельной чистой функцией — единственная нетривиальная часть
     * backpressure, покрывается unit-тестом без Kafka.
     *
     * <p>Гистерезис (возобновляем не на потолке, а на его половине)
     * осознанный: без него счётчик колеблется вокруг потолка и цикл на
     * каждой итерации дёргает pause/resume, что заодно сбрасывает
     * накопленный фетч.
     */
    static boolean shouldPause(int inFlight, int maxInFlight, boolean currentlyPaused) {
        if (currentlyPaused) {
            return inFlight > maxInFlight / 2;
        }
        return inFlight >= maxInFlight;
    }

    public static void run(
        KafkaConsumer<String, byte[]> consumer,
        KafkaProducer<String, byte[]> producer,
        MessageContextStore contextStore,
        GatewayRegistry gatewayRegistry,
        ControlSnapshot controlSnapshot,
        OperatorSubmitClient submitClient,
        SubmitIdempotencyStore idempotencyStore,
        AtomicBoolean running
    ) {
        // Реальная находка (нагрузочный прогон 1000 msg/s, JFR подтвердил
        // отсутствие CPU-хотспота — сервис большую часть времени ждёт, не
        // считает): последовательная обработка — Redis fetch(msgctx) ->
        // Redis resolve(gateway) -> БЛОКИРУЮЩИЙ gRPC submit (реальный SMPP
        // round-trip до SMSC через operator-smpp-session-manager) -> Kafka
        // produce().get() -> следующая запись. Все зависимости
        // (MessageContextStore/GatewayRegistry/SubmitIdempotencyStore — один
        // общий Lettuce-коннекшн, OperatorSubmitClient — ManagedChannel в
        // ConcurrentHashMap, KafkaProducer — потокобезопасен по документации
        // клиента) уже были сделаны потокобезопасными в более ранних правках.
        // Самокалибрующийся размер пула вместо статического
        // DELIVERY_CONCURRENCY — тот же класс фикса, что billing-service
        // (см. javadoc AdaptiveThreadPoolCalibrator).
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
            AdaptiveThreadPoolCalibrator.FLOOR, AdaptiveThreadPoolCalibrator.FLOOR,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> {
                Thread t = new Thread(r, "delivery-worker");
                t.setDaemon(true);
                return t;
            });
        AdaptiveThreadPoolCalibrator calibrator = new AdaptiveThreadPoolCalibrator(
            pool,
            envInt("DELIVERY_CONCURRENCY_CEILING", 512),
            envInt("THREAD_POOL_CALIBRATION_WINDOW_MS", 120_000),
            envInt("THREAD_POOL_CALIBRATION_TICK_MS", 10_000),
            // DELIVERY_CONCURRENCY снова читается. С появлением калибратора эта
            // переменная перестала использоваться где-либо в коде, но осталась
            // в docker-compose.yml с комментариями, описывающими её как
            // рабочую ручку — то есть висела мёртвой настройкой, которая
            // выглядела живой. Возвращаем ей исходный смысл: минимальная
            // конкурентность обработки. Дефолт FLOOR сохраняет прежнее
            // поведение, если переменная не задана.
            envInt("DELIVERY_CONCURRENCY", AdaptiveThreadPoolCalibrator.FLOOR));

        // Потолок незавершённых записей. Раньше эту роль неявно играл размер
        // батча (следующий poll не начинался, пока батч не слит), теперь
        // ограничение нужно явное — иначе непрерывный опрос вычитает топик в
        // память целиком. Считать по Литтлу так же, как пол пула: потолок
        // должен быть заметно больше, чем concurrency, иначе воркерам
        // нечего брать, но не настолько, чтобы держать в памяти минуты
        // трафика. 1024 при 300 TPS — примерно 3.4с трафика.
        int maxInFlight = Math.max(1, envInt("DELIVERY_MAX_IN_FLIGHT", DEFAULT_MAX_IN_FLIGHT));
        long commitIntervalMs = Math.max(50, envInt("KAFKA_COMMIT_INTERVAL_MS", 1000));
        int maxAttempts = Math.max(1, envInt("DELIVERY_MAX_ATTEMPTS", DEFAULT_MAX_ATTEMPTS));

        OffsetWatermarkTracker tracker = new OffsetWatermarkTracker();
        ClaimReleaser releaser = new ClaimReleaser(idempotencyStore);
        Queue<Completion> completions = new ConcurrentLinkedQueue<>();
        AtomicInteger inFlight = new AtomicInteger();
        // Отдельный однопоточный планировщик только под ретраи: пауза перед
        // повтором не должна занимать ни воркер (их и так ровно столько,
        // сколько выдерживает оператор), ни поток опроса (он обязан
        // продолжать poll(), иначе группа перебалансируется).
        ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "delivery-retry");
            t.setDaemon(true);
            return t;
        });

        Dispatcher dispatcher = new Dispatcher(
            pool, retryScheduler, calibrator, contextStore, gatewayRegistry, controlSnapshot,
            submitClient, idempotencyStore, producer, completions, inFlight, maxAttempts);

        consumer.subscribe(List.of(INPUT_TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // Колбэк выполняется ВНУТРИ poll(), то есть в потоке опроса —
                // работать с consumer здесь безопасно, партиции ещё наши.
                // Дожидаться in-flight не пытаемся: это может занять больше
                // rebalance timeout и уронить группу целиком. Коммитим то,
                // что уже завершено, а незавершённые записи будут переданы
                // заново новому владельцу — штатный at-least-once. reset()
                // гасит эпоху, чтобы «опоздавшие» завершения не пометили
                // готовыми оффсеты, которые к тому моменту уже
                // перечитываются заново.
                applyCompletions(completions, tracker, releaser);
                commitOffsetsSync(consumer, tracker.committableOffsets());
                tracker.reset();
                // Ключи идемпотентности отбираемых партиций НЕ освобождаем,
                // хотя commitOffsetsSync только что отработал: он не сообщает
                // об успехе, а незавершённые записи этих партиций в любом
                // случае передоставятся новому владельцу. Ключ там —
                // единственное, что удержит его от повторного submit'а.
                // Забываем их: освобождать эти оффсеты уже не наше дело,
                // они уйдут по 24-часовому TTL. Ребаланс редок, цена мала.
                releaser.forget(partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // Новые партиции приходят «resumed»; фактическое состояние
                // pause/resume пересчитывается на каждой итерации цикла ниже.
            }
        });

        boolean paused = false;
        long lastCommitMs = System.currentTimeMillis();
        try {
            while (running.get()) {
                // (1) Применяем завершения — ТОЛЬКО в этом потоке: трекер не
                // потокобезопасен намеренно, см. его javadoc.
                applyCompletions(completions, tracker, releaser);

                // (2) Backpressure. poll() вызывается ВСЕГДА, даже когда мы
                // «не хотим» новых записей: пропуск poll() старше
                // max.poll.interval.ms выкидывает консьюмера из группы и
                // вызывает ребаланс. Штатный механизм для этого случая —
                // pause() на назначенных партициях: poll() продолжает
                // ходить и слать heartbeat, но записей не отдаёт.
                boolean wantPause = shouldPause(inFlight.get(), maxInFlight, paused);
                Set<TopicPartition> assignment = consumer.assignment();
                if (!assignment.isEmpty()) {
                    if (wantPause) {
                        consumer.pause(assignment); // идемпотентно; покрывает и партиции, добавленные ребалансом
                    } else if (paused) {
                        consumer.resume(assignment);
                    }
                }
                paused = wantPause;

                // (3) Опрос. Ничего не ждёт: обработка предыдущей пачки может
                // быть ещё в разгаре.
                ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);

                // (4) Раздача. Регистрация оффсета обязана произойти ДО
                // диспатча: завершение может прилететь из колбэка Kafka/Lettuce
                // практически мгновенно, а трекеру нужен зарегистрированный
                // watermark, иначе завершение будет отброшено как «чужое».
                for (ConsumerRecord<String, byte[]> record : records) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    tracker.register(tp, record.offset());
                    inFlight.incrementAndGet();
                    dispatcher.dispatch(record, tp, tracker.epoch(), 1);
                }

                // (5) Коммит по таймеру и асинхронно. Не на каждую пачку и не
                // синхронно — именно синхронный per-batch коммит был второй
                // половиной прежней проблемы (см. негативный результат с
                // max.poll.records=64 в javadoc класса).
                long now = System.currentTimeMillis();
                if (now - lastCommitMs >= commitIntervalMs) {
                    lastCommitMs = now;
                    commitProgressAsync(consumer, tracker, releaser);
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            if (running.get()) {
                throw e;
            }
            // Штатная остановка: wakeup() из shutdown-хука прервал poll().
        } finally {
            // Graceful shutdown: дать in-flight записям договорить, применить
            // их завершения и синхронно закоммитить итог. Без этого при каждом
            // деплое переобрабатывалось бы до maxInFlight записей — то есть до
            // maxInFlight повторных публикаций stage.completed.
            drainInFlight(completions, tracker, inFlight, releaser);
            // Финальный коммит — по ВСЕМ watermark'ам, а не только по
            // «изменившимся»: последний commitAsync мог не долететь, повторить
            // его дешевле, чем передоставлять сообщения.
            commitOffsetsSync(consumer, tracker.allWatermarks());
            retryScheduler.shutdownNow();
            pool.shutdown();
        }
    }

    /** Переносит завершения из очереди воркеров в трекер. Только поток опроса. */
    private static void applyCompletions(Queue<Completion> completions, OffsetWatermarkTracker tracker,
                                         ClaimReleaser releaser) {
        Completion completion;
        while ((completion = completions.poll()) != null) {
            tracker.complete(completion.epoch(), completion.partition(), completion.offset());
            // Ключ ещё НЕ освобождается: запись лишь обработана, оффсет не
            // закоммичен. Освобождение — в колбэке подтверждённого коммита,
            // см. ClaimReleaser.
            releaser.track(completion.partition(), completion.offset(), completion.claimedStageExecutionId());
        }
    }

    private static void commitProgressAsync(KafkaConsumer<String, byte[]> consumer, OffsetWatermarkTracker tracker,
                                            ClaimReleaser releaser) {
        Map<TopicPartition, Long> committable = tracker.committableOffsets();
        if (committable.isEmpty()) {
            return; // прогресса с прошлого раза нет — брокера не тревожим
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        committable.forEach((tp, offset) -> offsets.put(tp, new OffsetAndMetadata(offset)));
        // Помечаем отправленным ДО подтверждения брокера сознательно: если
        // коммит не долетит, следующий (уже с бОльшим watermark'ом) его
        // перекроет — потери нет, потому что коммит монотонный и
        // накопительный. Единственный сценарий с последствиями — падение
        // процесса ровно между неудачным коммитом и следующим: тогда часть
        // записей передоставится, что at-least-once и так допускает.
        tracker.markCommitted(committable);
        consumer.commitAsync(offsets, (committed, exception) -> {
            if (exception != null) {
                // Ключи идемпотентности НЕ освобождаем: коммит не подтверждён,
                // записи ещё могут передоставиться, и без ключа это дало бы
                // повторный SMS абоненту. Они освободятся следующим успешным
                // коммитом — он накопительный и перекроет эти же оффсеты.
                LOG.log(Level.WARNING, "commitAsync не удался, следующий коммит перекроет: " + committed, exception);
                return;
            }
            releaser.releaseCommitted(committable);
        });
    }

    private static void commitOffsetsSync(KafkaConsumer<String, byte[]> consumer, Map<TopicPartition, Long> watermarks) {
        if (watermarks.isEmpty()) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        watermarks.forEach((tp, offset) -> offsets.put(tp, new OffsetAndMetadata(offset)));
        // Две попытки — не «на всякий случай», а из-за конкретного механизма:
        // wakeup() из shutdown-хука мог быть вызван, когда поток опроса НЕ
        // стоял в poll(). Тогда флаг одноразово срабатывает на ближайшем
        // блокирующем вызове — то есть ровно на этом финальном коммите, и
        // потерять его здесь означало бы переобработать всё, что успели
        // сделать. Флаг съедается первой попыткой, вторая проходит.
        for (int attempt = 1; attempt <= 2; attempt++) {
            try {
                consumer.commitSync(offsets, FINAL_COMMIT_TIMEOUT);
                return;
            } catch (org.apache.kafka.common.errors.WakeupException wakeup) {
                if (attempt == 2) {
                    LOG.log(Level.WARNING, "синхронный коммит прерван wakeup дважды: " + offsets, wakeup);
                }
            } catch (Exception e) {
                // Не фатально: незакоммиченное будет передоставлено.
                LOG.log(Level.WARNING, "синхронный коммит оффсетов не удался: " + offsets, e);
                return;
            }
        }
    }

    /**
     * Ждёт, пока все in-flight записи договорят, периодически применяя их
     * завершения. Ограничено по времени: подвисший оператор не должен
     * превращать остановку пода в бесконечную.
     */
    private static void drainInFlight(Queue<Completion> completions, OffsetWatermarkTracker tracker,
                                      AtomicInteger inFlight, ClaimReleaser releaser) {
        long deadline = System.currentTimeMillis() + SHUTDOWN_DRAIN_TIMEOUT.toMillis();
        while (inFlight.get() > 0 && System.currentTimeMillis() < deadline) {
            applyCompletions(completions, tracker, releaser);
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        // Воркер кладёт завершение в очередь ДО декремента счётчика, поэтому
        // inFlight==0 гарантирует, что очередь уже содержит всё.
        applyCompletions(completions, tracker, releaser);
        int stuck = inFlight.get();
        if (stuck > 0) {
            LOG.warning("остановка по таймауту дренажа: " + stuck
                + " записей не завершились, их оффсеты не коммитятся — будут переданы заново");
        }
    }

    /**
     * Раздача записей в пул и обработка их завершения. Всё, что здесь
     * происходит после {@code dispatch}, выполняется в worker-потоке, в
     * колбэке Kafka-продюсера или в event loop'е Lettuce — но НИКОГДА в
     * потоке опроса и никогда с обращением к {@link KafkaConsumer}.
     */
    private static final class Dispatcher {
        private final ThreadPoolExecutor pool;
        private final ScheduledExecutorService retryScheduler;
        private final AdaptiveThreadPoolCalibrator calibrator;
        private final MessageContextStore contextStore;
        private final GatewayRegistry gatewayRegistry;
        private final ControlSnapshot controlSnapshot;
        private final OperatorSubmitClient submitClient;
        private final SubmitIdempotencyStore idempotencyStore;
        private final Producer<String, byte[]> producer;
        private final Queue<Completion> completions;
        private final AtomicInteger inFlight;
        private final int maxAttempts;

        Dispatcher(
            ThreadPoolExecutor pool,
            ScheduledExecutorService retryScheduler,
            AdaptiveThreadPoolCalibrator calibrator,
            MessageContextStore contextStore,
            GatewayRegistry gatewayRegistry,
            ControlSnapshot controlSnapshot,
            OperatorSubmitClient submitClient,
            SubmitIdempotencyStore idempotencyStore,
            Producer<String, byte[]> producer,
            Queue<Completion> completions,
            AtomicInteger inFlight,
            int maxAttempts
        ) {
            this.pool = pool;
            this.retryScheduler = retryScheduler;
            this.calibrator = calibrator;
            this.contextStore = contextStore;
            this.gatewayRegistry = gatewayRegistry;
            this.controlSnapshot = controlSnapshot;
            this.submitClient = submitClient;
            this.idempotencyStore = idempotencyStore;
            this.producer = producer;
            this.completions = completions;
            this.inFlight = inFlight;
            this.maxAttempts = maxAttempts;
        }

        void dispatch(ConsumerRecord<String, byte[]> record, TopicPartition partition, long epoch, int attempt) {
            try {
                submitToPool(record, partition, epoch, attempt);
            } catch (java.util.concurrent.RejectedExecutionException rejected) {
                // Пул уже остановлен (пришли из отложенного ретрая после
                // shutdown) — оффсет не завершаем, запись передоставится.
                LOG.log(Level.WARNING, "пул остановлен, запись не будет обработана: partition="
                    + partition + " offset=" + record.offset(), rejected);
                inFlight.decrementAndGet();
            }
        }

        private void submitToPool(ConsumerRecord<String, byte[]> record, TopicPartition partition, long epoch, int attempt) {
            pool.execute(() -> {
                PendingAcks acks;
                long startNanos = System.nanoTime();
                try {
                    acks = processRecord(
                        record, contextStore, gatewayRegistry, controlSnapshot, submitClient, idempotencyStore, producer);
                } catch (Throwable t) {
                    fail(record, partition, epoch, attempt, t);
                    return;
                }
                calibrator.recordTaskLatency((System.nanoTime() - startNanos) / 1_000_000);
                // Ключевой момент новой схемы: воркер НЕ ждёт подтверждений.
                // Он освобождается сразу после реальной работы (gRPC submit),
                // а оффсет становится завершённым в колбэке — когда пришли и
                // Kafka-ack публикации stage.completed, и подтверждение
                // записи исхода в Redis. Гарантия «не коммитим
                // неопубликованное» ровно та же, что была у прежнего
                // блокирующего дренажа, но никто ради неё не стоит в park'е
                // (по JFR это было ~30мс на вызов, 15% времени воркеров).
                acks.allAcked().whenComplete((ignored, error) -> {
                    if (error != null) {
                        fail(record, partition, epoch, attempt, error);
                    } else {
                        finish(partition, epoch, record.offset(), acks.claimedStageExecutionId());
                    }
                });
            });
        }

        /**
         * Запись не обработалась. Ограниченное число повторов — на
         * транзиентное (недоступный Redis, таймаут ack, отвалившийся канал);
         * повторный реальный submit оператору при этом исключён
         * {@link SubmitIdempotencyStore} (claim -> AlreadyDone/
         * AmbiguousInFlight), поэтому ретрай безопасен.
         *
         * <p><b>Poison pill.</b> Когда повторы исчерпаны, оффсет всё равно
         * помечается завершённым и watermark продвигается. Это сознательный
         * выбор, а не недосмотр: watermark по определению коммитит только
         * непрерывный префикс, поэтому ОДНА принципиально необрабатываемая
         * запись иначе запинала бы коммит партиции НАВСЕГДА — лаг растёт,
         * при рестарте всё перечитывается с той же записи, и так по кругу.
         * Этот класс бага в этой же сессии уже ловили дважды (pipeline-engine
         * и policy-service), поэтому здесь он закрыт явно. Цена: такая
         * запись теряется (at-most-once ровно для неё) — она логируется на
         * SEVERE со всеми координатами (partition/offset/key), чтобы её можно
         * было переиграть вручную. Правильное место для неё —
         * {@code stage.delivery.dlq}, который запланирован, но продюсера сюда
         * ещё не подключён (см. javadoc класса); когда он появится,
         * публикация в DLQ встаёт ровно сюда, ПЕРЕД finish().
         */
        private void fail(ConsumerRecord<String, byte[]> record, TopicPartition partition, long epoch, int attempt, Throwable error) {
            if (attempt < maxAttempts) {
                LOG.log(Level.WARNING, "stage.delivery запись не обработана (попытка " + attempt + "/" + maxAttempts
                    + ") partition=" + partition + " offset=" + record.offset() + ", повтор", error);
                try {
                    retryScheduler.schedule(
                        () -> dispatch(record, partition, epoch, attempt + 1),
                        RETRY_BACKOFF_MS * attempt, TimeUnit.MILLISECONDS);
                    return;
                } catch (java.util.concurrent.RejectedExecutionException rejected) {
                    // Планировщик уже остановлен (идёт shutdown) — падаем в
                    // ветку ниже, оффсет не коммитится, запись передоставится.
                    LOG.log(Level.WARNING, "повтор невозможен, идёт остановка", rejected);
                    inFlight.decrementAndGet();
                    return;
                }
            }
            LOG.log(Level.SEVERE, "stage.delivery запись не обработана за " + maxAttempts
                + " попыток — считаем неисправимой (poison pill), watermark продвигается, запись ТЕРЯЕТСЯ: partition="
                + partition + " offset=" + record.offset() + " key=" + record.key(), error);
            finish(partition, epoch, record.offset(), null);
        }

        private void finish(TopicPartition partition, long epoch, long offset, String claimedStageExecutionId) {
            // Порядок важен: сначала в очередь, потом декремент. Дренаж при
            // остановке считает inFlight==0 достаточным доказательством того,
            // что очередь уже содержит все завершения.
            completions.add(new Completion(epoch, partition, offset, claimedStageExecutionId));
            inFlight.decrementAndGet();
        }
    }

    /**
     * package-private (не private) — тот же принцип, что
     * {@code billing-service/KafkaIo.processRecord}: напрямую тестируется
     * {@code KafkaIoProcessRecordTest} без живого {@link KafkaConsumer}.
     * {@code Producer<String, byte[]>}, не конкретный {@link KafkaProducer} —
     * позволяет подставить {@link org.apache.kafka.clients.producer.MockProducer}
     * в тестах, {@link #run} по-прежнему передаёт сюда настоящий {@link KafkaProducer}.
     */
    static PendingAcks processRecord(
        ConsumerRecord<String, byte[]> record,
        MessageContextStore contextStore,
        GatewayRegistry gatewayRegistry,
        ControlSnapshot controlSnapshot,
        OperatorSubmitClient submitClient,
        SubmitIdempotencyStore idempotencyStore,
        Producer<String, byte[]> producer
    ) throws Exception {
        StageExecuteCommand command = StageExecuteCommand.parseFrom(record.value());
        DeliveryExtension extension = command.getDelivery();

        // check_control_state — повторная проверка OPERATOR_ROUTE перед submit.
        // HOLD не публикует stage.completed здесь — Scheduler Standard Lane
        // владеет решением, когда отпустить (см. DeliveryService.isAdmitted).
        if (!DeliveryService.isAdmitted(controlSnapshot.check(extension.getRouteId()))) {
            LOG.info("stage_execution_id=" + command.getStageExecutionId() + " held по OPERATOR_ROUTE control state, submit отложен");
            return PendingAcks.NONE; // HOLD: stage.completed не публикуется, ждать нечего
        }

        // Детерминированный queue_msg_id (по stage_execution_id, не
        // UUID.randomUUID() на каждый вызов) — исправление CRITICAL находки
        // кодревью: hld.md §21 требует "stable stage_execution_id" как одну
        // из опор дедупликации; случайный id на каждую редеставку эту опору
        // ломал. См. SubmitIdempotencyStore.
        String stageExecutionId = command.getStageExecutionId();
        String deterministicQueueMsgId = "dlv-" + stageExecutionId;

        // Фаза 11 плана закрытия API-пробелов: sandbox — до gatewayRegistry.resolve,
        // ни contextStore.fetch (не нужен, submit не строится), ни
        // idempotencyStore.claim, ни submitClient.submit не вызываются — оба
        // операторских шлюза структурно недостижимы для sandbox-трафика.
        // Синтетический успех + синтетический DLR публикуются тут же, одним
        // branch'ем (см. DeliveryService.buildSandboxDeliveryStatusEvent).
        if (command.getSandbox()) {
            SubmitOutcome sandboxOutcome = new SubmitOutcome(Outcome.OUTCOME_SUCCEEDED, "", "SANDBOX-" + deterministicQueueMsgId);
            StageCompletedEvent event = DeliveryService.buildEvent(command, deterministicQueueMsgId, sandboxOutcome);
            producer.send(new ProducerRecord<>(OUTPUT_TOPIC, event.getMessageId(), event.toByteArray()))
                .get(PRODUCER_SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            DeliveryStatusEvent dlrEvent = DeliveryService.buildSandboxDeliveryStatusEvent(command, extension, Instant.now());
            producer.send(new ProducerRecord<>(DELIVERY_STATUS_TOPIC, dlrEvent.getMessageId(), dlrEvent.toByteArray()))
                .get(PRODUCER_SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            return PendingAcks.NONE; // sandbox — редкий путь, две публикации ждём на месте
        }

        // fetch и resolve НЕЗАВИСИМЫ — раньше выполнялись последовательно и
        // стоили два park+wakeup (~25мс каждый по JFR). Отправляем обе команды
        // сразу, ждём один раз.
        io.lettuce.core.RedisFuture<java.util.Map<String, String>> ctxFuture =
            contextStore.fetchAsync(command.getMessageId());
        io.lettuce.core.RedisFuture<java.util.Map<String, String>> gwFuture =
            gatewayRegistry.resolveAsync(extension.getResolvedOperatorId(), extension.getRouteId());

        MessageContext context = MessageContextStore.toContext(ctxFuture.get());
        if (context == null) {
            // HIGH находка кодревью: раньше это был throw, который блокировал
            // партицию НАВСЕГДА, если msgctx реально никогда не появится
            // (TTL-эвикция/несогласованность), в отличие от gateway-not-found
            // ниже, который корректно публикует stage.completed с UNKNOWN.
            // command уже успешно распарсен — message_id/stage_execution_id
            // валидны, StageCompletedEvent построить можно; нет причины
            // блокировать всю партицию ради одной записи с недостающим
            // контекстом, если можно завершить её тем же путём, что и любой
            // другой неопределённый исход submit'а.
            LOG.warning("stage_execution_id=" + stageExecutionId + " MessageContext не найден для "
                + command.getMessageId() + ", завершаем как SUBMISSION_OUTCOME_UNKNOWN вместо блокировки партиции");
            SubmitOutcome outcome = DeliveryService.handleGrpcFailure("MESSAGE_CONTEXT_NOT_FOUND");
            StageCompletedEvent event = DeliveryService.buildEvent(command, deterministicQueueMsgId, outcome);
            return new PendingAcks(sendCompletedEvent(producer, event), Collections.emptyList(), null);
        }

        GatewayEndpoint endpoint = GatewayRegistry.toEndpoint(gwFuture.get());

        SubmitOutcome outcome;
        String queueMsgId;
        List<RedisFuture<?>> pendingRedis = Collections.emptyList();
        if (endpoint == null) {
            // Registry-запись отсутствует (владеющая реплика ещё не
            // зарегистрировалась/сдохла без переизбрания) — тот же исход,
            // что неопределённый результат submit, не считается FAILED
            // (нельзя утверждать, что оператор отклонил бы сообщение).
            // Нет реального side effect — claim через идемпотентный store не
            // нужен, безопасно ретраится сколько угодно раз.
            queueMsgId = deterministicQueueMsgId;
            outcome = DeliveryService.handleGrpcFailure("GATEWAY_INSTANCE_NOT_FOUND");
        } else {
            SubmitIdempotencyStore.ClaimResult claim = idempotencyStore.claim(stageExecutionId, deterministicQueueMsgId);
            if (claim instanceof SubmitIdempotencyStore.ClaimResult.AlreadyDone already) {
                // Реальный submit уже состоялся при более ранней попытке
                // (эта — редеставка после сбоя публикации stage.completed) —
                // повторный вызов submitClient.submit(...) исключён, исход
                // переиспользуется как есть.
                queueMsgId = already.queueMsgId();
                outcome = already.outcome();
            } else if (claim instanceof SubmitIdempotencyStore.ClaimResult.AmbiguousInFlight ambiguous) {
                // Claim уже занят, но исход не записан — крэш между реальным
                // submit и записью результата. Нельзя утверждать, дошёл ли
                // submit до оператора: hld.md §21 явно запрещает
                // автоматический повторный submit при UNKNOWN, поэтому здесь
                // НЕ вызываем submitClient.submit(...) снова.
                queueMsgId = ambiguous.queueMsgId();
                outcome = DeliveryService.handleGrpcFailure("AMBIGUOUS_PRIOR_ATTEMPT_NOT_RESUBMITTED");
                pendingRedis = idempotencyStore.recordOutcomeAsync(stageExecutionId, outcome, null);
            } else {
                SubmitIdempotencyStore.ClaimResult.Won won = (SubmitIdempotencyStore.ClaimResult.Won) claim;
                queueMsgId = won.queueMsgId();
                List<Segment> segments = SegmentMessage.segment(context.body(), context.encoding());
                SubmitRequest request = DeliveryService.buildSubmitRequest(command, extension, context, queueMsgId, segments);
                // Момент ДО submit'а, не после — см. SubmitIdempotencyStore.recordOutcome:
                // DLR Manager отбрасывает запись быстрого пути, чей submitted_at
                // позже received_at самой DLR, а при мгновенно отвечающем SMSC
                // "после submit'а" регулярно оказывается позже.
                Instant submitStartedAt = Instant.now();
                try {
                    SubmitResponse response = submitClient.submit(endpoint.endpoint(), request);
                    outcome = DeliveryService.interpretSubmitResult(response);
                } catch (io.grpc.StatusRuntimeException e) {
                    outcome = DeliveryService.handleGrpcFailure(e.getStatus().getCode().name());
                }
                // segment_id=1 — тот же номер, что проставляют обе стороны
                // durable-пути (OperatorSubmitServer.dispatchOne и DLR-сторона
                // в operator-smpp-session-manager/Main.java); ключ корреляции
                // обязан сойтись байт в байт, поэтому здесь та же константа,
                // а не число сегментов.
                pendingRedis = idempotencyStore.recordOutcomeAsync(stageExecutionId, outcome,
                    new SubmitIdempotencyStore.CorrelationHint(
                        extension.getResolvedOperatorId(), command.getMessageId(), 1, submitStartedAt));
            }
        }

        StageCompletedEvent event = DeliveryService.buildEvent(command, queueMsgId, outcome);
        return new PendingAcks(sendCompletedEvent(producer, event), pendingRedis, stageExecutionId);
    }

    /**
     * Подтверждения, которые ещё не дождались: публикация stage.completed и
     * запись исхода в идемпотентный store. Оба ожидаются ПЕРЕД тем, как
     * оффсет записи станет завершённым — воркер на них не стоит.
     *
     * <p>По JFR это два park'а по ~28-30мс каждый в самом хвосте обработки,
     * когда реальная работа (gRPC submit оператору) уже сделана. Гарантии не
     * ослабляются: оффсет не двигается, пока и Kafka, и Redis не подтвердили.
     * Раньше их ждал блокирующий цикл дренажа (и вместе с ними стоял весь
     * следующий poll), теперь — {@link CompletableFuture} без единого
     * занятого потока.
     */
    /**
     * @param claimedStageExecutionId ключ {@code dlvsubmit:{stage_execution_id}},
     *        реально захваченный этой записью, либо {@code null}, если
     *        {@code claim} не вызывался. Нужен, чтобы освободить ключ после
     *        ПОДТВЕРЖДЁННОГО коммита оффсета — см. {@link #releaseClaims}.
     *        Пути HOLD, sandbox и раннего REJECTED (нет MessageContext) claim
     *        не берут вовсе, у них здесь {@code null}.
     */
    record PendingAcks(CompletableFuture<RecordMetadata> kafka, List<RedisFuture<?>> redis,
                       String claimedStageExecutionId) {
        static final PendingAcks NONE = new PendingAcks(null, Collections.emptyList(), null);

        /**
         * Один future на оба подтверждения. {@code RedisFuture} у Lettuce —
         * это {@code CompletionStage}, поэтому композиция бесплатна и
         * колбэк выполняется на event loop'е Lettuce / в IO-потоке
         * продюсера, а не на нашем воркере.
         */
        CompletableFuture<Void> allAcked() {
            List<CompletableFuture<?>> all = new ArrayList<>(1 + redis.size());
            if (kafka != null) {
                all.add(kafka);
            }
            for (RedisFuture<?> future : redis) {
                all.add(future.toCompletableFuture());
            }
            if (all.isEmpty()) {
                return CompletableFuture.completedFuture(null); // HOLD/sandbox — ждать нечего
            }
            return CompletableFuture.allOf(all.toArray(CompletableFuture[]::new))
                // Без таймаута одно потерянное подтверждение держало бы
                // watermark партиции вечно (см. ACK_TIMEOUT).
                .orTimeout(ACK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Общий helper для обоих мест публикации {@code stage.completed} в
     * {@link #processRecord} (путь MESSAGE_CONTEXT_NOT_FOUND и обычный путь).
     */
    private static CompletableFuture<RecordMetadata> sendCompletedEvent(Producer<String, byte[]> producer, StageCompletedEvent event) {
        // Раньше здесь был .get(): воркер ПАРКОВАЛСЯ на подтверждение брокера.
        // JFR (300 TPS) намерил 359с из 2435с суммарного park-времени именно
        // тут, в среднем 30мс на вызов — 15% всего времени воркеров, при том
        // что сама публикация ничего не решает для дальнейшей обработки.
        // Callback-вариант send() вместо Future: Future пришлось бы кому-то
        // ждать блокирующе (ровно то, что мы убираем), а колбэк отдаёт
        // управление IO-потоку продюсера — здесь он только доводит
        // CompletableFuture, ничего тяжёлого не делает.
        CompletableFuture<RecordMetadata> ack = new CompletableFuture<>();
        producer.send(new ProducerRecord<>(OUTPUT_TOPIC, event.getMessageId(), event.toByteArray()),
            (metadata, exception) -> {
                if (exception != null) {
                    ack.completeExceptionally(exception);
                } else {
                    ack.complete(metadata);
                }
            });
        return ack;
    }
}
