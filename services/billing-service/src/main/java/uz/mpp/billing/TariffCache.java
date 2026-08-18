package uz.mpp.billing;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Per-partner тариф (Фаза 5a плана закрытия API-пробелов: multi-tenancy в
 * Billing Service). До этого класса {@link TariffResolver} был один
 * глобальный экземпляр на весь сервис, читался один раз при старте из
 * статического файла — задокументированное упрощение "Фазы 2.2"
 * ({@link BillingService} javadoc).
 *
 * <p>На cache miss — синхронный point-lookup в Configuration Redis
 * ({@link RedisUrl#buildConfigurationUrl()}), ключи ровно те, что уже
 * пишет {@code config-cache-projector} для {@code entity_type=billing_tariff}
 * ({@code config:current:billing_tariff:{partner_id}} → номер версии, затем
 * {@code config:version:billing_tariff:{partner_id}:{version}} → JSON
 * payload). Результат кэшируется в памяти навсегда (нет TTL/инвалидации по
 * {@code config.changes} в этом проходе — см. {@link #invalidate}, явный
 * hook под будущий консьюмер, не реализованный здесь, задокументировано
 * как отдельная работа в плане).
 *
 * <p>Если для партнёра ничего не найдено в Configuration Redis (ни один
 * {@code BILLING_TARIFF} config version никогда не публиковался —
 * реальное состояние текущего локального walking-skeleton) —
 * graceful fallback на статический дефолтный тариф ({@code defaultResolver},
 * тот же {@code BILLING_TARIFF_PATH}-файл, что раньше был единственным
 * источником), не hard-fail заряда.
 */
public final class TariffCache {

    private static final Logger LOG = Logger.getLogger(TariffCache.class.getName());

    private final RedisClient client;
    private final TariffResolver defaultResolver;
    private final ConcurrentHashMap<String, TariffResolver> cache = new ConcurrentHashMap<>();

    public TariffCache(String configurationRedisUrl, TariffResolver defaultResolver) {
        this.client = RedisClient.create(configurationRedisUrl);
        this.defaultResolver = defaultResolver;
    }

    public void close() {
        client.shutdown();
    }

    public TariffResolver resolve(String partnerId) {
        return cache.computeIfAbsent(partnerId, this::loadFromRedisOrDefault);
    }

    /** Hook для будущего config.changes-консьюмера (не реализован в этом проходе, см. class javadoc). */
    public void invalidate(String partnerId) {
        cache.remove(partnerId);
    }

    private TariffResolver loadFromRedisOrDefault(String partnerId) {
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> commands = connection.sync();
            String currentVersion = commands.get(currentKey(partnerId));
            if (currentVersion == null || currentVersion.isEmpty()) {
                LOG.info(() -> "BILLING_TARIFF не найден в Configuration Redis для partner_id=" + partnerId + " — используется дефолтный тариф");
                return defaultResolver;
            }
            String payload = commands.get(versionKey(partnerId, currentVersion));
            if (payload == null || payload.isEmpty()) {
                LOG.warning(() -> "config:current указывает на версию " + currentVersion + ", но config:version пуст для partner_id=" + partnerId + " — используется дефолтный тариф");
                return defaultResolver;
            }
            return TariffResolver.fromConfigSchemaJson(payload);
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, e, () -> "не удалось прочитать BILLING_TARIFF для partner_id=" + partnerId + " из Configuration Redis — используется дефолтный тариф");
            return defaultResolver;
        }
    }

    private static String currentKey(String partnerId) {
        return "config:current:billing_tariff:" + partnerId;
    }

    private static String versionKey(String partnerId, String version) {
        return "config:version:billing_tariff:" + partnerId + ":" + version;
    }
}
