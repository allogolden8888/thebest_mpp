package uz.mpp.billing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Реальный round-trip против локального Configuration Redis (не мок) — тот
 * же принцип, что {@link BillingAccountStoreTest}. Seed ровно тех ключей,
 * что реально пишет {@code config-cache-projector} для
 * {@code entity_type=billing_tariff} (Фаза 5a плана закрытия API-пробелов).
 */
class TariffCacheTest {

    private static final String REDIS_URL = System.getenv().getOrDefault("TARIFF_CACHE_TEST_REDIS_URL", "redis://localhost:6379/0");

    private RedisClient rawClient;
    private TariffCache cache;
    private TariffResolver defaultResolver;
    private String partnerId;

    @BeforeEach
    void setUp() {
        rawClient = RedisClient.create(REDIS_URL);
        defaultResolver = TariffResolver.fromConfigSchemaJson(
            "{\"currency\":\"UZS\",\"price_per_segment\":{\"BLOCKED\":1,\"DEFAULT\":1},\"default_category\":\"DEFAULT\"}");
        cache = new TariffCache(REDIS_URL, defaultResolver);
        partnerId = "test-partner-" + UUID.randomUUID();
    }

    @AfterEach
    void tearDown() {
        try (StatefulRedisConnection<String, String> conn = rawClient.connect()) {
            conn.sync().del(currentKey(partnerId), versionKey(partnerId, "1"), versionKey(partnerId, "2"));
        }
        cache.close();
        rawClient.shutdown();
    }

    private static String currentKey(String partnerId) {
        return "config:current:billing_tariff:" + partnerId;
    }

    private static String versionKey(String partnerId, String version) {
        return "config:version:billing_tariff:" + partnerId + ":" + version;
    }

    private void seed(String version, String payloadJson) {
        try (StatefulRedisConnection<String, String> conn = rawClient.connect()) {
            conn.sync().set(currentKey(partnerId), version);
            conn.sync().set(versionKey(partnerId, version), payloadJson);
        }
    }

    @Test
    void unseededPartnerFallsBackToDefaultResolver() {
        TariffResolver resolved = cache.resolve(partnerId);
        assertSame(defaultResolver, resolved, "без BILLING_TARIFF в Configuration Redis обязан вернуться дефолтный резолвер, не упасть");
    }

    @Test
    void seededPartnerResolvesRealTariffFromRedis() {
        seed("1", "{\"currency\":\"UZS\",\"price_per_segment\":{\"BLOCKED\":94,\"TRANSACTION\":50},\"default_category\":\"TRANSACTION\"}");

        TariffResolver resolved = cache.resolve(partnerId);
        var tariff = resolved.resolve("TRANSACTION", 3);
        assertEquals(150, tariff.amountMinorUnits(), "3 сегмента * 50 = 150, из тарифа, реально прочитанного из Redis, не дефолтного");
        assertEquals("UZS", tariff.currencyCode());
    }

    @Test
    void resolveIsCachedNotReReadOnEveryCall() {
        seed("1", "{\"currency\":\"UZS\",\"price_per_segment\":{\"BLOCKED\":94,\"TRANSACTION\":50},\"default_category\":\"TRANSACTION\"}");
        TariffResolver first = cache.resolve(partnerId);

        // Меняем данные в Redis "из-под" кэша — если бы resolve() перечитывал
        // на каждый вызов, второй вызов увидел бы новую цену (100). Кэш без
        // инвалидации (см. TariffCache javadoc) обязан вернуть ТОТ ЖЕ объект.
        seed("2", "{\"currency\":\"UZS\",\"price_per_segment\":{\"BLOCKED\":94,\"TRANSACTION\":100},\"default_category\":\"TRANSACTION\"}");
        TariffResolver second = cache.resolve(partnerId);

        assertSame(first, second, "повторный resolve() для того же partner_id обязан вернуть закэшированный экземпляр, не перечитывать Redis");
        assertEquals(150, second.resolve("TRANSACTION", 3).amountMinorUnits(), "закэшированное значение не должно было измениться");
    }

    @Test
    void invalidateForcesReReadOnNextResolve() {
        seed("1", "{\"currency\":\"UZS\",\"price_per_segment\":{\"BLOCKED\":94,\"TRANSACTION\":50},\"default_category\":\"TRANSACTION\"}");
        cache.resolve(partnerId);

        seed("2", "{\"currency\":\"UZS\",\"price_per_segment\":{\"BLOCKED\":94,\"TRANSACTION\":100},\"default_category\":\"TRANSACTION\"}");
        cache.invalidate(partnerId);
        TariffResolver afterInvalidate = cache.resolve(partnerId);

        assertEquals(300, afterInvalidate.resolve("TRANSACTION", 3).amountMinorUnits(), "после invalidate следующий resolve() обязан перечитать актуальные данные");
    }
}
