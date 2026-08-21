package uz.mpp.billingoutbox;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RedisUrlTest {

    /**
     * Главный регрессионный случай: пароль ОБЯЗАН попасть в URI. Раньше
     * Main.java собирал строку без него, из-за чего сервис не мог
     * подключиться к защищённому Redis ни локально, ни в проде.
     */
    @Test
    void includesPasswordWhenSet() {
        Map<String, String> env = Map.of(
            "REDIS_BILLING_HOST", "redis-billing",
            "REDIS_BILLING_PORT", "6379",
            "REDIS_BILLING_PASSWORD", "mpp_local_dev");
        assertEquals("redis://:mpp_local_dev@redis-billing:6379/0", RedisUrl.buildBillingUrl(env::get));
    }

    @Test
    void omitsPasswordSectionWhenUnset() {
        Map<String, String> env = Map.of("REDIS_BILLING_HOST", "redis-billing", "REDIS_BILLING_PORT", "6379");
        assertEquals("redis://redis-billing:6379/0", RedisUrl.buildBillingUrl(env::get));
    }

    @Test
    void explicitUrlOverrideWins() {
        Map<String, String> env = Map.of(
            "REDIS_BILLING_URL", "redis://custom:1234/5",
            "REDIS_BILLING_HOST", "ignored");
        assertEquals("redis://custom:1234/5", RedisUrl.buildBillingUrl(env::get));
    }

    @Test
    void fallsBackToPlatformDefaultHost() {
        assertEquals("redis://redis-billing.mpp.svc:6379/0", RedisUrl.buildBillingUrl(k -> null));
    }
}
