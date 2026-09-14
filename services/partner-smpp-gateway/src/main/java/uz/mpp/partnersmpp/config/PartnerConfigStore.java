package uz.mpp.partnersmpp.config;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import uz.mpp.partnersmpp.config.PartnerConfigLoader.SmppBindCredential;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Заменяет одноразовый {@code PartnerConfigLoader.fromFile(PARTNER_CONFIG_PATH)}
 * (см. {@code Main.java} до этой правки, README "Что НЕ реализовано" —
 * "hot-reload/config.changes consumer не реализован, тот же паттерн
 * упрощения, что у Routing/Policy/partner-rest-receiver/
 * partner-notification-service") реальным живым источником — тот же
 * Configuration Redis, что уже пишет {@code config-cache-projector} (см.
 * {@code services/config-cache-projector/README.md}) и уже читает
 * {@code billing-service}'s {@code TariffCache} (тот же point-lookup
 * паттерн: {@code config:current:{entity_type}:{id}} → номер версии, затем
 * {@code config:version:{entity_type}:{id}:{version}} → JSON payload,
 * {@code "partner"} — тот же lower_snake_case entity_type, что
 * {@code config-cache-projector/internal/projector/projector.go
 * entityTypeString(CONFIG_ENTITY_TYPE_PARTNER)} пишет).
 *
 * <p><b>Два независимых пути записи в {@link #current}:</b>
 * <ul>
 *   <li>{@link #bootstrap()} — вызывается один раз при старте, {@code SCAN}
 *   по {@code config:current:partner:*} находит ВСЕХ партнёров (в этом
 *   Redis нет отдельного индекса "список партнёров" — {@code
 *   config-cache-projector} пишет только per-partner ключи), читает
 *   каждого и строит объединённую {@code system_id -> credential} карту
 *   сразу для всех партнёров, разом заменяя пустую карту (нет частичного
 *   видимого состояния во время бутстрапа).</li>
 *   <li>{@link #refreshPartner(String)} — вызывается на каждый {@code
 *   entity_type=PARTNER config.changes} (см. {@link
 *   uz.mpp.partnersmpp.kafkaio.ConfigChangeConsumer}), перечитывает ОДНОГО
 *   партнёра и атомарно (compare-and-swap, {@link AtomicReference#updateAndGet})
 *   заменяет только его записи в живой карте — остальные партнёры не
 *   трогаются, не нужен полный re-bootstrap на каждое изменение.</li>
 * </ul>
 *
 * <p>Обе операции читают Redis, не подписываются на payload из самого
 * Kafka-события напрямую (в отличие от, например, {@code
 * routing-service/src/config_reload.rs}, который применяет {@code
 * event.payload_json} прямо в оверлей) — намеренный выбор: Configuration
 * Redis уже является объявленным единым источником истины для этого
 * среза (см. README config-cache-projector), re-fetch по {@code entity_id}
 * гарантирует, что мы видим ровно то же самое "текущее" состояние, что
 * увидел бы любой другой bootstrap-читатель (например, свежий под после
 * рестарта), а не рассинхронизированную копию, собранную только из потока
 * событий этого процесса.
 *
 * <p><b>КРИТИЧНО — почему {@link #refreshPartner(String, String)} принимает
 * {@code status} события, а не только {@code partnerId}.</b> {@code
 * config-cache-projector/internal/projector/projector.go WriteProjection}
 * обновляет {@code config:current:partner:{id}} ТОЛЬКО когда {@code
 * event.status == "active"} (её же комментарий: "архивная версия не должна
 * становиться текущей для bootstrap hot-path сервисов"). Значит, когда
 * партнёр архивируется, {@code config:current} НЕ переставляется на
 * архивную версию — он застревает на номере ПОСЛЕДНЕЙ активной версии.
 * Точечный re-fetch по {@code config:current} (как делает {@link
 * #loadPartner}) в таком случае молча вернул бы СТАРЫЕ (всё ещё активные)
 * credential'ы — партнёр остался бы бинд-способным НАВСЕГДА после
 * архивации, полностью сводя на нет весь смысл этой задачи. Обнаружено при
 * реализации, не в исходном плане — та же категория проблемы, что уже
 * задокументирована в этом файле для {@code routing-service/src/
 * config_reload.rs} ("применяет event.payload_json прямо в оверлей, не
 * re-fetch'ит Redis") — судя по всему, та реализация как раз обходит этот
 * gotcha, используя статус из САМОГО события, а не из {@code
 * config:current}.
 *
 * <p>Исправление здесь: {@link #refreshPartner(String, String)} различает
 * два случая по {@code status} события (сам {@code
 * ConfigChangeEvent.status}, см. {@link
 * uz.mpp.partnersmpp.kafkaio.ConfigChangeConsumer}):
 * <ul>
 *   <li>{@code status == "active"} — событие говорит, что ЭТА версия и
 *   есть новая активная, значит {@code config-cache-projector} уже успел
 *   переставить {@code config:current} — безопасно делать point re-fetch,
 *   как и раньше (гарантирует ровно то же "текущее" состояние, что увидел
 *   бы любой другой bootstrap-читатель).</li>
 *   <li>{@code status != "active"} (в первую очередь {@code "archived"}) —
 *   САМО событие уже говорит нам достаточно: этот партнёр только что
 *   перестал быть активным. Redis {@code config:current} не поможет (см.
 *   выше — не переставлен), поэтому вместо чтения оттуда мы напрямую
 *   убираем все записи этого {@code partnerId} из живой карты — тот же
 *   конечный эффект, что должен был быть, без ложного доверия
 *   несуществующему обновлению Redis. Новые bind'ы этим {@code system_id}
 *   перестают аутентифицироваться сразу после этого события, не после
 *   какого-то отдельного, никогда не наступающего перехода в active.</li>
 * </ul>
 * Kafka-порядок гарантирован: {@code config-event-publisher} ключует
 * записи по {@code entity_id} (её {@code internal/kafkaio/publisher.go
 * Publish}), так что все события ОДНОГО партнёра идут в одну партицию —
 * "active" и "archived" события для одного {@code partnerId} никогда не
 * будут обработаны не в том порядке, в каком их произвёл {@code
 * configuration-service}.
 *
 * <p>См. {@code Main.java} за разбором, что происходит с УЖЕ держащейся
 * TCP-сессией архивированного партнёра — этот класс сознательно НЕ решает
 * этот вопрос сам (только поддерживает живую auth-карту), принудительное
 * разъединение — забота вызывающего кода ({@code Main.java}).
 */
public final class PartnerConfigStore implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(PartnerConfigStore.class.getName());
    private static final String CURRENT_KEY_PREFIX = "config:current:partner:";

    private final RedisClient client;
    private final AtomicReference<Map<String, SmppBindCredential>> current = new AtomicReference<>(Map.of());

    public PartnerConfigStore(String configurationRedisUrl) {
        this.client = RedisClient.create(configurationRedisUrl);
    }

    /** Снапшот живой карты — читается синхронно на каждый SMPP bind ({@link uz.mpp.partnersmpp.server.VaultAuthenticator}), никогда не блокирует на Redis. */
    public Map<String, SmppBindCredential> currentCredentials() {
        return current.get();
    }

    /**
     * Полный первичный обход — вызывается один раз в {@code Main.main()} до
     * старта SMPP-сервера (тот же порядок, что был у {@code fromFile}: bind
     * не должен начать приниматься, пока хотя бы этот единственный обход не
     * завершился).
     */
    public void bootstrap() {
        Map<String, SmppBindCredential> merged = new LinkedHashMap<>();
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> commands = connection.sync();
            ScanArgs args = ScanArgs.Builder.matches(CURRENT_KEY_PREFIX + "*").limit(500);
            KeyScanCursor<String> cursor = commands.scan(args);
            while (true) {
                for (String key : cursor.getKeys()) {
                    String partnerId = key.substring(CURRENT_KEY_PREFIX.length());
                    try {
                        merged.putAll(loadPartner(commands, partnerId));
                    } catch (RuntimeException e) {
                        LOG.log(Level.WARNING, e, () -> "bootstrap: не удалось загрузить partner_id=" + partnerId + " из Configuration Redis, пропущен в этом обходе");
                    }
                }
                if (cursor.isFinished()) {
                    break;
                }
                cursor = commands.scan(cursor, args);
            }
        } catch (RuntimeException e) {
            LOG.log(Level.SEVERE, e, () -> "bootstrap: не удалось перечислить партнёров в Configuration Redis — стартуем с пустой SMPP-картой (ни один bind не пройдёт, пока не придёт первое config.changes событие)");
            current.set(Map.of());
            return;
        }
        current.set(Map.copyOf(merged));
        LOG.info(() -> "bootstrap: загружено " + merged.size() + " auth.type=SMPP_BIND credential(ов) из Configuration Redis");
    }

    /** {@code ConfigChangeEvent.status}, ожидаемое от config-cache-projector/configuration-service как "текущий активен". */
    private static final String STATUS_ACTIVE = "active";

    /**
     * Точечный re-fetch/удаление одного партнёра — вызывается из {@link
     * uz.mpp.partnersmpp.kafkaio.ConfigChangeConsumer} на каждое {@code
     * entity_type=PARTNER config.changes} событие, с {@code status} этого
     * САМОГО события (не текущим состоянием Redis).
     *
     * <p>{@code status.equals("active")} — Redis {@code config:current} уже
     * переставлен на эту версию ({@code config-cache-projector} гарантирует
     * это, см. класс-докстринг "КРИТИЧНО"), point re-fetch безопасен и даёт
     * ровно то состояние, что видел бы любой другой bootstrap-читатель.
     *
     * <p>Иначе (архивация и т.п.) — Redis {@code config:current} НЕ будет
     * переставлен (та же причина), поэтому читать оттуда бессмысленно и
     * опасно (вернуло бы устаревшие, всё ещё "активные" credential'ы) —
     * вместо этого напрямую убираем все записи {@code partnerId} из живой
     * карты, доверяя статусу из самого события (упорядоченность гарантирует
     * Kafka-ключ {@code entity_id}, см. класс-докстринг).
     *
     * <p>Транзиентная ошибка Redis на активной ветке НЕ обнуляет живую
     * карту — оставляем последнее известное хорошее состояние (тот же
     * fail-safe принцип, что {@link uz.mpp.partnersmpp.server.VaultAuthenticator}
     * применяет к своему Vault-кешу) и полагаемся на at-least-once
     * редоставку Kafka: offset этого события не коммитится, следующий poll
     * попробует снова. Ветка архивации не обращается к Redis вообще, ей
     * нечему транзиентно отказать.
     */
    public void refreshPartner(String partnerId, String status) {
        if (!STATUS_ACTIVE.equals(status)) {
            int removed = current.updateAndGet(prev -> {
                Map<String, SmppBindCredential> next = new LinkedHashMap<>();
                for (Map.Entry<String, SmppBindCredential> entry : prev.entrySet()) {
                    if (!entry.getValue().partnerId().equals(partnerId)) {
                        next.put(entry.getKey(), entry.getValue());
                    }
                }
                return Map.copyOf(next);
            }).size();
            LOG.info(() -> "refreshPartner: partner_id=" + partnerId + " status=" + status
                + " (не active) — все SMPP_BIND credential(ы) этого партнёра убраны из живой карты без обращения к Redis"
                + " (config:current не переставляется на неактивную версию, см. PartnerConfigStore javadoc), карта теперь содержит "
                + removed + " credential(ов) всего");
            return;
        }

        Map<String, SmppBindCredential> partnerCreds;
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            partnerCreds = loadPartner(connection.sync(), partnerId);
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, e, () -> "refreshPartner: не удалось перечитать partner_id=" + partnerId
                + " из Configuration Redis — живая карта не тронута, ждём следующей редоставки config.changes");
            throw e;
        }

        current.updateAndGet(prev -> {
            Map<String, SmppBindCredential> next = new LinkedHashMap<>();
            for (Map.Entry<String, SmppBindCredential> entry : prev.entrySet()) {
                if (!entry.getValue().partnerId().equals(partnerId)) {
                    next.put(entry.getKey(), entry.getValue());
                }
            }
            next.putAll(partnerCreds);
            return Map.copyOf(next);
        });
        LOG.info(() -> "refreshPartner: partner_id=" + partnerId + " обновлён, " + partnerCreds.size() + " SMPP_BIND credential(ов) сейчас активны для него");
    }

    private Map<String, SmppBindCredential> loadPartner(RedisCommands<String, String> commands, String partnerId) {
        String currentVersion = commands.get(currentKey(partnerId));
        if (currentVersion == null || currentVersion.isEmpty()) {
            return Map.of();
        }
        String payload = commands.get(versionKey(partnerId, currentVersion));
        if (payload == null || payload.isEmpty()) {
            LOG.warning(() -> "config:current указывает на версию " + currentVersion + ", но config:version пуст для partner_id=" + partnerId);
            return Map.of();
        }
        return PartnerConfigLoader.fromJson(payload);
    }

    private static String currentKey(String partnerId) {
        return CURRENT_KEY_PREFIX + partnerId;
    }

    private static String versionKey(String partnerId, String version) {
        return "config:version:partner:" + partnerId + ":" + version;
    }

    @Override
    public void close() {
        client.shutdown();
    }
}
