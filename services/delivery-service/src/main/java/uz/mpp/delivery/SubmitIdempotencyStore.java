package uz.mpp.delivery;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import uz.mpp.delivery.DeliveryService.SubmitOutcome;
import uz.mpp.platformcontracts.common.v1.Outcome;

/**
 * Закрывает CRITICAL находку кодревью (PART 2, delivery-service #1):
 * {@code queueMsgId} был свежим {@code UUID.randomUUID()} на каждый вызов
 * {@code processRecord}, включая редеставку того же {@code stage_execution_id}
 * после успешного gRPC submit, но до успешной публикации {@code stage.completed}
 * — редеставка вызывала настоящий повторный submit оператору (дубль SMS).
 *
 * Реализует ровно то, что требует {@code hld.md §21}: "Дубли предотвращаются
 * сочетанием: stable stage_execution_id; CAS в Runtime Redis; ...; запрета
 * автоматического повторного submit при UNKNOWN." — атомарный claim
 * ({@code HSETNX}) на {@code stage_execution_id} гарантирует, что реальный
 * {@code submitClient.submit(...)} вызывается не более одного раза за весь
 * TTL-жизни ключа; если claim уже занят, а результат ещё не записан (крэш
 * между submit и записью исхода), это НЕ ретраится как новый submit — по
 * правилу платформы такой случай трактуется как {@code SUBMISSION_OUTCOME_UNKNOWN},
 * тем же queue_msg_id, что был в исходной (возможно реально дошедшей до
 * оператора) попытке.
 *
 * MEDIUM находка кодревью: то же соединение-на-вызов, что было в
 * {@code MessageContextStore}/{@code GatewayRegistry} — исправлено тем же
 * способом, один общий {@code StatefulRedisConnection} на весь жизненный
 * цикл сервиса. {@code concurrentClaimsOnSameStageExecutionIdOnlyOneWinner}
 * (SubmitIdempotencyStoreTest) уже бьёт по этому же общему соединению из
 * 20 потоков одновременно — Lettuce документированно потокобезопасен для
 * такого использования, тест это подтверждает поведенчески.
 */
public final class SubmitIdempotencyStore {

    private static final Logger LOG = Logger.getLogger(SubmitIdempotencyStore.class.getName());

    private static final Duration TTL = Duration.ofHours(24);

    /**
     * Ожидание ответа Redis в {@link #recordOutcome}. Тот же порядок, что
     * PRODUCER_SEND_TIMEOUT в KafkaIo — воркер не должен висеть тут вечно,
     * если Redis деградировал.
     */
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(5);

    /**
     * TTL быстрого пути корреляции DLR. НЕ равен окну корреляции
     * (DLR_CORRELATION_WINDOW, 48ч) СОЗНАТЕЛЬНО, см. подробное обоснование
     * на {@link #correlationKey}.
     */
    static final Duration DEFAULT_FAST_PATH_TTL = Duration.ofMinutes(15);

    private final Duration fastPathTtl;

    /**
     * Всё, что нужно записать в быстрый путь корреляции сверх того, что уже
     * несёт {@link SubmitOutcome}. {@code submittedAt} — момент НЕПОСРЕДСТВЕННО
     * ПЕРЕД вызовом {@code OperatorSubmitService.Submit}, не после: см.
     * {@link #recordOutcome}.
     */
    public record CorrelationHint(String operatorId, String messageId, int segmentId, Instant submittedAt) {
    }

    /** Итог, который нужно применить в {@code processRecord}. */
    public sealed interface ClaimResult {
        record Won(String queueMsgId) implements ClaimResult {
        }

        record AlreadyDone(SubmitOutcome outcome, String queueMsgId) implements ClaimResult {
        }

        record AmbiguousInFlight(String queueMsgId) implements ClaimResult {
        }
    }

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;

    /** {@code submit_claim.lua} — ресурс classpath, тем же способом, что {@code BillingAccountStore}. */
    private static final String CLAIM_SCRIPT = loadScript("submit_claim.lua");

    /**
     * SHA1 скрипта считается локально, а не через {@code SCRIPT LOAD}: Redis
     * определяет digest ровно как SHA1 тела скрипта, поэтому отдельный
     * round trip на загрузку не нужен — первый же {@code EVAL} кладёт скрипт
     * в кэш под этим самым digest'ом.
     */
    private static final String CLAIM_SHA = sha1Hex(CLAIM_SCRIPT);

    /**
     * Известно ли, что скрипт уже лежит в кэше ЭТОГО Redis-инстанса.
     * {@code false} -> идём через {@code EVAL} (он и загрузит), дальше — через
     * {@code EVALSHA} (не гоняем тело скрипта по сети на каждое сообщение).
     *
     * Сбрасывается обратно в {@code false} по {@code NOSCRIPT}: после
     * рестарта Redis скрипт-кэш пуст (равно как и после {@code SCRIPT FLUSH}
     * или переключения на другую реплику), и без этого фоллбэка claim падал
     * бы на КАЖДОМ сообщении до рестарта сервиса — сценарий не гипотетический,
     * переживаемость рестарта Redis на этом стенде уже была отдельной
     * проблемой.
     */
    private volatile boolean claimScriptCached = false;

    public SubmitIdempotencyStore(String redisUrl) {
        this(redisUrl, DEFAULT_FAST_PATH_TTL);
    }

    public SubmitIdempotencyStore(String redisUrl, Duration fastPathTtl) {
        this.client = RedisClient.create(redisUrl);
        this.connection = client.connect();
        this.fastPathTtl = fastPathTtl;
    }

    private static String key(String stageExecutionId) {
        return "dlvsubmit:" + stageExecutionId;
    }

    /**
     * Ключ БЫСТРОГО ПУТИ корреляции DLR — читается DLR Manager'ом
     * ({@code dlr-manager/internal/correlation.Store.Lookup}) ПЕРЕД
     * PostgreSQL. Формат обязан совпадать с Go-стороной байт в байт:
     * {@code dlrcorr:{operator_id}:{smsc_message_id}:{segment_id}}.
     *
     * ЗАЧЕМ ЭТО ВООБЩЕ ЕСТЬ (измерено, не гипотеза). SMSC в этой инсталляции
     * стоит в одной сети и отвечает мгновенно: в Kafka соседние записи
     * {@code operator.submit.accepted} и {@code operator.dlr} по одному и тому
     * же {@code smsc_message_id} имеют ОДИН И ТОТ ЖЕ CreateTime (миллисекунда
     * в миллисекунду). Durable-путь корреляции при этом асинхронный:
     * {@code operator.submit.accepted} -> dlr-correlation-writer копит батч и
     * флашит его в {@code dlr.dlr_correlation} раз в BATCH_FLUSH_INTERVAL_MS
     * (2 секунды). DLR приходит в DLR Manager раньше, чем строка появляется в
     * PostgreSQL, {@code Lookup} возвращает "не найдено", решение —
     * ScheduleRetry, DLR уходит в {@code dlr:pending:*} + Runtime Redis и
     * ретраится. Замерено на прогоне 300 TPS: 27000 отправленных сообщений
     * дали +8298 записей в {@code delivery.status}, а Runtime Redis вырос на
     * 99658 ключей (3.7 ключа на сообщение) — сообщения не финализируются.
     *
     * ПОЧЕМУ ЗДЕСЬ, а не в operator-smpp-session-manager (который и есть
     * источник {@code smsc_message_id}):
     *   1) этот вызов уже происходит — {@code recordOutcome} уже пишет в
     *      Runtime Redis сразу после возврата gRPC submit, и {@code outcome}
     *      уже несёт {@code smsc_message_id}; добавляется ОДНА команда в тот
     *      же конвейер, нового блокирующего хопа нет (более того, метод стал
     *      быстрее, см. {@link #recordOutcome});
     *   2) JFR-профиль на 300 TPS показал, что gRPC submit — 60% времени
     *      воркеров operator-smpp-session-manager; добавлять туда блокирующий
     *      Redis-вызов в путь SMPP-диспетчеризации нельзя;
     *   3) delivery-service — ЕДИНСТВЕННАЯ точка, общая для обоих операторских
     *      транспортов (SMPP и operator-http-gateway реализуют один и тот же
     *      {@code OperatorSubmitService}), поэтому одна правка здесь
     *      покрывает оба, а правка в SMPP-менеджере покрыла бы только SMPP.
     *
     * ПОЧЕМУ TTL 15 МИНУТ, А НЕ 48 ЧАСОВ (окно корреляции). Быстрый путь
     * закрывает ровно интервал "submit состоялся, но durable-строки в
     * PostgreSQL ещё нет" — это BATCH_FLUSH_INTERVAL_MS (2с) плюс отставание
     * консьюмера dlr-correlation-writer. Всё, что приходит позже, и так
     * находится в PostgreSQL, ради чего durable-путь и существует. TTL в 48ч
     * стоил бы при 300 TPS порядка 52 млн ключей в Runtime Redis — а
     * нехватка памяти на этой VM и есть та проблема, ради которой всё
     * затевалось; 15 минут ограничивают быстрый путь ~270 тыс. ключей.
     *
     * Значение — компактная строка, а не хэш: {@code SET ... EX} это ОДНА
     * команда (HSET+EXPIRE — две), и строковое представление в Redis
     * заметно дешевле по памяти, чем хэш из трёх полей.
     */
    static String correlationKey(String operatorId, String smscMessageId, int segmentId) {
        return "dlrcorr:" + operatorId + ":" + smscMessageId + ":" + segmentId;
    }

    /** Формат значения быстрого пути; разбирается Go-стороной через SplitN(v, "|", 3). */
    static String correlationValue(Instant submittedAt, String messageId, String stageExecutionId) {
        return submittedAt.toEpochMilli() + "|" + messageId + "|" + stageExecutionId;
    }

    /**
     * {@code cas_transition}-подобный claim — один атомарный Lua-вызов
     * ({@code submit_claim.lua}, ресурс classpath), тем же способом, что
     * {@code apply_atomic_charge} в billing-service и
     * {@code cas_transition}/{@code finalize} в pipeline-engine.
     *
     * <p>Семантика не изменилась: вернуть, удалось ли захватить claim, а если
     * нет — переиспользовать уже записанный исход/queue_msg_id. Изменилось
     * ЧИСЛО round trip'ов и атомарность — полное обоснование (замеры, класс
     * утечки ключей без TTL) в шапке {@code submit_claim.lua}: раньше это были
     * две последовательные команды в КАЖДОЙ ветке (HSETNX+EXPIRE либо
     * HSETNX+HGETALL), и падение процесса между HSETNX и EXPIRE оставляло
     * ключ без TTL навсегда.
     */
    public ClaimResult claim(String stageExecutionId, String deterministicQueueMsgId) {
        String k = key(stageExecutionId);
        List<Object> result = evalClaim(k, deterministicQueueMsgId);

        if (result.isEmpty()) {
            // По контракту скрипта невозможно — но молча трактовать пустой
            // ответ как "claim захвачен" нельзя: это был бы реальный дубль
            // submit'а абоненту, ради предотвращения которого класс и написан.
            throw new IllegalStateException("submit_claim.lua вернул пустой ответ для " + k);
        }
        if ("WON".equals(result.get(0))) {
            return new ClaimResult.Won(deterministicQueueMsgId);
        }

        // {"EXISTS", field, value, ...} — плоский HGETALL, прочитанный тем же
        // неделимым шагом, что и HSETNX (см. шапку скрипта).
        Map<String, String> existing = new HashMap<>();
        for (int i = 1; i + 1 < result.size(); i += 2) {
            existing.put((String) result.get(i), (String) result.get(i + 1));
        }

        String queueMsgId = existing.getOrDefault("queue_msg_id", deterministicQueueMsgId);
        if ("DONE".equals(existing.get("status"))) {
            Outcome outcome = Outcome.valueOf(existing.get("outcome"));
            SubmitOutcome cached = new SubmitOutcome(
                outcome, existing.getOrDefault("reason_code", ""), existing.getOrDefault("smsc_message_id", ""));
            return new ClaimResult.AlreadyDone(cached, queueMsgId);
        }
        return new ClaimResult.AmbiguousInFlight(queueMsgId);
    }

    /**
     * {@code EVALSHA} с фоллбэком на {@code EVAL} по {@code NOSCRIPT} —
     * см. {@link #claimScriptCached} про рестарт Redis.
     */
    private List<Object> evalClaim(String key, String deterministicQueueMsgId) {
        RedisCommands<String, String> commands = connection.sync();
        String[] keys = {key};
        String ttlSeconds = String.valueOf(TTL.toSeconds());

        if (claimScriptCached) {
            try {
                return commands.evalsha(CLAIM_SHA, ScriptOutputType.MULTI, keys, deterministicQueueMsgId, ttlSeconds);
            } catch (RedisNoScriptException e) {
                claimScriptCached = false; // кэш скриптов Redis пуст — ниже EVAL заполнит его заново
            }
        }

        List<Object> result = commands.eval(CLAIM_SCRIPT, ScriptOutputType.MULTI, keys, deterministicQueueMsgId, ttlSeconds);
        claimScriptCached = true;
        return result;
    }

    private static String loadScript(String resourceName) {
        try (InputStream in = SubmitIdempotencyStore.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IllegalStateException(resourceName + " не найден в classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("не удалось прочитать " + resourceName, e);
        }
    }

    private static String sha1Hex(String script) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-1").digest(script.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 обязан быть доступен в любой JVM", e);
        }
    }

    /** Записывает исход состоявшегося (или трактованного как UNKNOWN) submit'а. */
    public void recordOutcome(String stageExecutionId, SubmitOutcome outcome) {
        recordOutcome(stageExecutionId, outcome, null);
    }

    /**
     * Записывает исход submit'а И, если {@code correlation} задан и оператор
     * синхронно вернул {@code smsc_message_id}, — запись быстрого пути
     * корреляции DLR (см. {@link #correlationKey}).
     *
     * ПРО СТОИМОСТЬ, это не абстрактное требование. JFR на 300 TPS: воркеры
     * этого сервиса тратят ~186мс блокировки на сообщение в шести
     * последовательных round-trip'ах, и каждый park+wakeup стоит ~25мс из-за
     * контеншена за CPU. Поэтому метод переведён с {@code sync()} на
     * {@code async()}: раньше это были ДВА ПОСЛЕДОВАТЕЛЬНЫХ round-trip'а
     * (HSET, затем EXPIRE — каждый со своим park'ом), теперь все команды,
     * включая новую SET быстрого пути, уходят в сокет подряд и ждутся ОДИН
     * раз. Итог: команд стало три вместо двух, а park'ов — один вместо двух.
     *
     * {@code submittedAt} берётся ПЕРЕД gRPC submit'ом (см. KafkaIo), а не
     * после, и это существенно для корректности: DLR Manager отбрасывает
     * запись быстрого пути, если её {@code submitted_at} позже
     * {@code received_at} самой DLR (защита от переиспользования
     * {@code smsc_message_id}, см. correlation/store.go). При мгновенно
     * отвечающем SMSC "время после возврата submit'а" регулярно оказывается
     * ПОЗЖЕ момента получения DLR, и такая запись отбрасывалась бы всегда —
     * то есть быстрый путь просто не работал бы. Момент НАЧАЛА submit'а
     * заведомо не позже реального submit'а на стороне оператора, а значит и
     * не позже любой DLR о нём.
     */
    public void recordOutcome(String stageExecutionId, SubmitOutcome outcome, CorrelationHint correlation) {
        List<RedisFuture<?>> pending = recordOutcomeAsync(stageExecutionId, outcome, correlation);
        awaitRecorded(pending);
    }

    /**
     * Тот же самый набор команд, но БЕЗ ожидания — futures отдаются наверх.
     *
     * Зачем. По JFR это ожидание стоило воркеру ~28мс park+wakeup, и лежало
     * оно в самом хвосте обработки: submit уже выполнен, событие построено,
     * дальше остаётся только опубликовать stage.completed. Держать на нём
     * поток незачем — тот же приём уже применён к публикации в Kafka
     * (см. KafkaIo.sendCompletedEvent).
     *
     * ГАРАНТИЯ СОХРАНЯЕТСЯ ПОЛНОСТЬЮ. Ожидание не исчезает, а переносится в
     * цикл дренажа KafkaIo — ПЕРЕД коммитом офсета. То есть офсет
     * по-прежнему не двигается, пока запись об исходе не подтверждена
     * Redis'ом. Это принципиально: без подтверждённой записи переобработка
     * той же записи Kafka могла бы привести к ПОВТОРНОМУ реальному submit'у
     * оператору, ради чего этот store и существует.
     */
    public List<RedisFuture<?>> recordOutcomeAsync(String stageExecutionId, SubmitOutcome outcome, CorrelationHint correlation) {
        RedisAsyncCommands<String, String> commands = connection.async();
        String k = key(stageExecutionId);

        List<RedisFuture<?>> pending = new ArrayList<>(3);
        pending.add(commands.hset(k, Map.of(
            "status", "DONE",
            "outcome", outcome.outcome().name(),
            "reason_code", outcome.reasonCode() == null ? "" : outcome.reasonCode(),
            "smsc_message_id", outcome.smscMessageId() == null ? "" : outcome.smscMessageId())));
        pending.add(commands.expire(k, TTL));

        String smscMessageId = outcome.smscMessageId();
        if (correlation != null && smscMessageId != null && !smscMessageId.isEmpty()) {
            pending.add(commands.set(
                correlationKey(correlation.operatorId(), smscMessageId, correlation.segmentId()),
                correlationValue(correlation.submittedAt(), correlation.messageId(), stageExecutionId),
                SetArgs.Builder.ex(fastPathTtl)));
        }

        return pending;
    }

    /**
     * Освобождение ключей {@code dlvsubmit:{stage_execution_id}} записей,
     * оффсеты которых УЖЕ подтверждённо закоммичены.
     *
     * <p>Требование сформулировано явно: состояние сообщения, дошедшего до
     * терминальной стадии, не должно оставаться ни в Redis, ни в Kafka.
     * Раньше ключ жил до 24-часового TTL независимо от судьбы сообщения, и
     * Runtime Redis рос до 1.16ГБ — на этой машине нехватка памяти бьёт по
     * хвостам латентности напрямую. TTL остаётся страховкой ровно для того,
     * для чего и нужен: записи, не дошедшие до подтверждённого коммита.
     *
     * <p>{@code UNLINK}, а не {@code DEL}: освобождение памяти уходит в
     * фоновый поток Redis, вызывающий не платит за него на горячем пути.
     *
     * <p>Ждать здесь нечего и намеренно не ждём — потеря освобождения не
     * влияет на корректность (ключ доживёт до TTL), а поток опроса, из
     * которого это вызывается, обязан немедленно вернуться к {@code poll()}.
     * Ошибка логируется, но не пробрасывается: сорвать цикл опроса из-за
     * неудавшейся уборки было бы несоразмерно.
     *
     * <p><b>Вызывать ТОЛЬКО после подтверждённого коммита оффсета.</b>
     * Обоснование и разобранный опасный вариант — в javadoc
     * {@code KafkaIo.ClaimReleaser}: освобождение раньше коммита открывает
     * повторный submit тому же абоненту.
     */
    public void releaseAsync(Collection<String> stageExecutionIds) {
        if (stageExecutionIds == null || stageExecutionIds.isEmpty()) {
            return;
        }
        String[] keys = stageExecutionIds.stream().map(SubmitIdempotencyStore::key).toArray(String[]::new);
        try {
            connection.async().unlink(keys).exceptionally(error -> {
                LOG.log(Level.WARNING, "не удалось освободить " + keys.length
                    + " ключей dlvsubmit — уйдут по TTL", error);
                return null;
            });
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "освобождение ключей dlvsubmit не отправлено — уйдут по TTL", e);
        }
    }

    /** Ожидание записей, отданных {@link #recordOutcomeAsync}. */
    public static void awaitRecorded(List<RedisFuture<?>> pending) {
        if (pending == null || pending.isEmpty()) {
            return;
        }
        LettuceFutures.awaitAll(
            AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS, pending.toArray(new RedisFuture<?>[0]));
    }

    public void close() {
        connection.close();
        client.shutdown();
    }
}
