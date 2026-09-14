package uz.mpp.partnersmpp.config;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import uz.mpp.partnersmpp.config.PartnerConfigLoader.SmppBindCredential;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Живой {@code system_id -> SMPP credential} snapshot.
 *
 * <p>{@link #bootstrap()} загружает точные immutable-версии из
 * Configuration Redis перед стартом listener. Затем {@link #applyEvent}
 * применяет versioned {@code config.changes.payload_json} напрямую. Нельзя
 * перечитывать {@code config:current} на событие: cache-projector — другой
 * consumer и может ещё не спроецировать эту версию, а archived-версия вообще
 * не становится current. Per-partner version fence не даёт полному Kafka
 * replay откатить Redis snapshot или оживить архивированного партнёра.
 *
 * <p>Карта заменяется атомарно и никогда не выполняет Redis I/O на bind
 * hot-path. Разрыв уже открытых сессий архивированного партнёра выполняет
 * {@code Main} через общий {@code ChannelRegistry}.
 */
public final class PartnerConfigStore implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(PartnerConfigStore.class.getName());
    private static final String CURRENT_KEY_PREFIX = "config:current:partner:";

    private final RedisClient client;
    private final AtomicReference<Map<String, SmppBindCredential>> current = new AtomicReference<>(Map.of());
    private final Map<String, Long> currentVersions = new ConcurrentHashMap<>();
    private final Map<String, String> currentStatuses = new ConcurrentHashMap<>();
    // Lifecycle tombstone for the currently installed config version. It is
    // deliberately separate from payload.status: an active config version may
    // legitimately carry a suspended/archived partner document.
    private final Set<String> terminalArchives = ConcurrentHashMap.newKeySet();

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
        Map<String, Long> versions = new LinkedHashMap<>();
        Map<String, String> statuses = new LinkedHashMap<>();
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> commands = connection.sync();
            ScanArgs args = ScanArgs.Builder.matches(CURRENT_KEY_PREFIX + "*").limit(500);
            KeyScanCursor<String> cursor = commands.scan(args);
            while (true) {
                for (String key : cursor.getKeys()) {
                    String partnerId = key.substring(CURRENT_KEY_PREFIX.length());
                    try {
                        String version = commands.get(key);
                        if (version == null || version.isBlank()) {
                            throw new IllegalArgumentException("пустой current version");
                        }
                        long parsedVersion = Long.parseLong(version);
                        PartnerConfigLoader.ParsedPartner parsed = loadPartnerVersion(
                            commands,
                            partnerId,
                            version
                        );
                        rejectPartnerStatus(parsed.status());
                        if (!partnerId.equals(parsed.partnerId())) {
                            throw new IllegalArgumentException("config:version partner_id не совпадает с config:current key");
                        }
                        rejectCrossPartnerSystemIdCollision(merged, parsed.credentials());
                        merged.putAll(parsed.credentials());
                        versions.put(partnerId, parsedVersion);
                        statuses.put(partnerId, parsed.status());
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
            currentVersions.clear();
            currentStatuses.clear();
            terminalArchives.clear();
            return;
        }
        current.set(Map.copyOf(merged));
        currentVersions.clear();
        currentVersions.putAll(versions);
        currentStatuses.clear();
        currentStatuses.putAll(statuses);
        // Redis current pointers represent lifecycle-active config versions;
        // terminal archive state is reconstructed by the Kafka replay barrier.
        terminalArchives.clear();
        LOG.info(() -> "bootstrap: загружено " + merged.size() + " auth.type=SMPP_BIND credential(ов) из Configuration Redis");
    }

    /**
     * Applies the immutable PARTNER payload carried by {@code config.changes}.
     * Redis is intentionally not read here: config-cache-projector is an
     * independent consumer and may not have projected this version yet.
     * Version fencing also makes a full topic replay after Redis bootstrap
     * safe: older retained events cannot roll the local map back.
     */
    public synchronized boolean applyEvent(String partnerId, long version, String status, byte[] payloadJson) {
        if (partnerId == null || partnerId.isBlank()) {
            throw new IllegalArgumentException("PARTNER config event должен содержать entity_id");
        }
        if (version <= 0) {
            throw new IllegalArgumentException("PARTNER config event должен содержать положительную version");
        }
        long installedVersion = currentVersions.getOrDefault(partnerId, 0L);
        boolean sameVersionArchive = version == installedVersion
            && STATUS_ARCHIVED.equals(status)
            && !terminalArchives.contains(partnerId);
        if (version < installedVersion || (version == installedVersion && !sameVersionArchive)) {
            return false;
        }

        Map<String, SmppBindCredential> replacement;
        String effectivePartnerStatus;
        if (STATUS_ACTIVE.equals(status)) {
            if (payloadJson == null || payloadJson.length == 0) {
                throw new IllegalArgumentException("active PARTNER config event должен содержать payload_json");
            }
            PartnerConfigLoader.ParsedPartner parsed = PartnerConfigLoader.parseJson(
                new String(payloadJson, java.nio.charset.StandardCharsets.UTF_8)
            );
            if (!partnerId.equals(parsed.partnerId())) {
                throw new IllegalArgumentException("config.changes entity_id не совпадает с payload partner_id");
            }
            rejectPartnerStatus(parsed.status());
            replacement = parsed.credentials();
            effectivePartnerStatus = parsed.status();
        } else if (STATUS_ARCHIVED.equals(status)) {
            replacement = Map.of();
            effectivePartnerStatus = STATUS_ARCHIVED;
        } else {
            throw new IllegalArgumentException("неподдерживаемый PARTNER config status=" + status);
        }

        current.updateAndGet(previous -> {
            Map<String, SmppBindCredential> next = new LinkedHashMap<>();
            for (Map.Entry<String, SmppBindCredential> entry : previous.entrySet()) {
                if (!entry.getValue().partnerId().equals(partnerId)) {
                    next.put(entry.getKey(), entry.getValue());
                }
            }
            rejectCrossPartnerSystemIdCollision(next, replacement);
            next.putAll(replacement);
            return Map.copyOf(next);
        });
        currentVersions.put(partnerId, version);
        currentStatuses.put(partnerId, effectivePartnerStatus);
        if (STATUS_ARCHIVED.equals(status)) {
            terminalArchives.add(partnerId);
        } else {
            terminalArchives.remove(partnerId);
        }
        return true;
    }

    /** {@code ConfigChangeEvent.status}, ожидаемое от configuration-service как "текущий активен". */
    private static final String STATUS_ACTIVE = "active";
    private static final String STATUS_SUSPENDED = "suspended";
    private static final String STATUS_ARCHIVED = "archived";

    /** Effective partner payload status, distinct from config-version lifecycle status. */
    public String partnerStatus(String partnerId) {
        return currentStatuses.getOrDefault(partnerId, STATUS_ARCHIVED);
    }

    private static void rejectPartnerStatus(String status) {
        if (!STATUS_ACTIVE.equals(status)
            && !STATUS_SUSPENDED.equals(status)
            && !STATUS_ARCHIVED.equals(status)) {
            throw new IllegalArgumentException("неподдерживаемый partner payload status=" + status);
        }
    }

    private PartnerConfigLoader.ParsedPartner loadPartnerVersion(
        RedisCommands<String, String> commands,
        String partnerId,
        String version
    ) {
        String payload = commands.get(versionKey(partnerId, version));
        if (payload == null || payload.isEmpty()) {
            throw new IllegalArgumentException(
                "config:current указывает на версию " + version + ", но config:version пуст для partner_id=" + partnerId
            );
        }
        return PartnerConfigLoader.parseJson(payload);
    }

    private static void rejectCrossPartnerSystemIdCollision(
        Map<String, SmppBindCredential> installed,
        Map<String, SmppBindCredential> replacement
    ) {
        for (Map.Entry<String, SmppBindCredential> entry : replacement.entrySet()) {
            SmppBindCredential existing = installed.get(entry.getKey());
            if (existing != null && !existing.partnerId().equals(entry.getValue().partnerId())) {
                throw new IllegalArgumentException("SMPP system_id=" + entry.getKey()
                    + " одновременно объявлен партнёрами " + existing.partnerId()
                    + " и " + entry.getValue().partnerId());
            }
        }
    }

    private static String versionKey(String partnerId, String version) {
        return "config:version:partner:" + partnerId + ":" + version;
    }

    @Override
    public void close() {
        client.shutdown();
    }
}
