package uz.mpp.partnersmpp.server;

import org.junit.jupiter.api.Test;
import uz.mpp.partnersmpp.config.PartnerConfigLoader.SmppBindCredential;
import uz.mpp.partnersmpp.vault.VaultSecretReader;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link VaultAuthenticator} orchestration — фейковый {@link
 * VaultSecretReader} (детерминированный контроль над сетью), реальный
 * round-trip живёт отдельно в {@code VaultClientTest}. Покрывает ровно то,
 * что докстринг класса обещает: кеш-хит не зовёт сеть повторно, TTL
 * истёк -> зовёт снова, неизвестный system_id никогда не трогает Vault,
 * fail-closed на холодном кеше, stale-кеш переживает временную
 * недоступность Vault.
 */
class VaultAuthenticatorTest {

    private static final SmppBindCredential CRED =
        new SmppBindCredential("click_uz", "smpp_main", "vault://partners/click_uz/smpp_main/api_key");

    /** Считает вызовы, чтобы тесты могли утверждать "Vault не звался" / "звался ровно N раз". */
    private static final class CountingReader implements VaultSecretReader {
        final AtomicInteger calls = new AtomicInteger();
        private final java.util.function.Supplier<String> valueOrThrow;

        CountingReader(java.util.function.Supplier<String> valueOrThrow) {
            this.valueOrThrow = valueOrThrow;
        }

        static CountingReader returning(String value) {
            return new CountingReader(() -> value);
        }

        static CountingReader alwaysThrows() {
            return new CountingReader(() -> { throw new RuntimeException("Vault unreachable"); });
        }

        @Override
        public String readProperty(String kvPath, String property) {
            calls.incrementAndGet();
            return valueOrThrow.get();
        }
    }

    private static Map<String, SmppBindCredential> singleCredential() {
        return Map.of("CLICK", CRED);
    }

    @Test
    void unknownSystemIdRejectedWithoutEverCallingVault() {
        CountingReader vault = CountingReader.returning("irrelevant");
        VaultAuthenticator auth = new VaultAuthenticator(singleCredential(), vault);

        PartnerAuthenticator.AuthResult result = auth.authenticate("UNKNOWN_SYSTEM_ID", "any-password");

        assertFalse(result.allowed());
        assertEquals(0, vault.calls.get(), "неизвестный system_id не должен доходить до Vault вообще");
    }

    @Test
    void correctPasswordAccepted() {
        CountingReader vault = CountingReader.returning("real-secret");
        VaultAuthenticator auth = new VaultAuthenticator(singleCredential(), vault);

        PartnerAuthenticator.AuthResult result = auth.authenticate("CLICK", "real-secret");

        assertTrue(result.allowed());
        assertEquals("click_uz", result.partnerId());
        assertEquals("smpp_main", result.applicationId());
    }

    @Test
    void wrongPasswordRejected() {
        CountingReader vault = CountingReader.returning("real-secret");
        VaultAuthenticator auth = new VaultAuthenticator(singleCredential(), vault);

        PartnerAuthenticator.AuthResult result = auth.authenticate("CLICK", "wrong-password");

        assertFalse(result.allowed());
    }

    @Test
    void cacheHitWithinTtlDoesNotCallVaultAgain() {
        CountingReader vault = CountingReader.returning("real-secret");
        VaultAuthenticator auth = new VaultAuthenticator(singleCredential(), vault, Duration.ofSeconds(45), Clock.systemUTC());

        auth.authenticate("CLICK", "real-secret");
        auth.authenticate("CLICK", "real-secret");
        auth.authenticate("CLICK", "real-secret");

        assertEquals(1, vault.calls.get(), "три вызова подряд в пределах TTL должны дать ровно один реальный Vault-read");
    }

    @Test
    void ttlExpiryTriggersFreshRead() {
        CountingReader vault = CountingReader.returning("real-secret");
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        java.util.concurrent.atomic.AtomicReference<Instant> now = new java.util.concurrent.atomic.AtomicReference<>(t0);
        Clock movableClock = new Clock() {
            @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId zone) { return this; }
            @Override public Instant instant() { return now.get(); }
        };
        VaultAuthenticator auth = new VaultAuthenticator(singleCredential(), vault, Duration.ofSeconds(45), movableClock);

        auth.authenticate("CLICK", "real-secret");
        now.set(t0.plusSeconds(46)); // за пределами TTL=45с
        auth.authenticate("CLICK", "real-secret");

        assertEquals(2, vault.calls.get(), "после истечения TTL следующий authenticate должен перечитать Vault");
    }

    @Test
    void coldCacheVaultUnavailableFailsClosed() {
        CountingReader vault = CountingReader.alwaysThrows();
        VaultAuthenticator auth = new VaultAuthenticator(singleCredential(), vault);

        PartnerAuthenticator.AuthResult result = auth.authenticate("CLICK", "any-password");

        assertFalse(result.allowed(), "холодный кеш + недоступный Vault обязаны дать reject, не разрешение по умолчанию");
    }

    @Test
    void staleCacheServesLastKnownValueWhenVaultBecomesUnavailable() {
        java.util.concurrent.atomic.AtomicBoolean shouldFail = new java.util.concurrent.atomic.AtomicBoolean(false);
        VaultSecretReader vault = (kvPath, property) -> {
            if (shouldFail.get()) {
                throw new RuntimeException("Vault temporarily unreachable");
            }
            return "real-secret";
        };
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        java.util.concurrent.atomic.AtomicReference<Instant> now = new java.util.concurrent.atomic.AtomicReference<>(t0);
        Clock movableClock = new Clock() {
            @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId zone) { return this; }
            @Override public Instant instant() { return now.get(); }
        };
        VaultAuthenticator auth = new VaultAuthenticator(singleCredential(), vault, Duration.ofSeconds(45), movableClock);

        // Первый bind — успешный read, кладёт значение в кеш.
        PartnerAuthenticator.AuthResult first = auth.authenticate("CLICK", "real-secret");
        assertTrue(first.allowed());

        // TTL истёк И Vault временно недоступен — должны использовать протухший кеш, не отказывать.
        now.set(t0.plusSeconds(46));
        shouldFail.set(true);
        PartnerAuthenticator.AuthResult second = auth.authenticate("CLICK", "real-secret");

        assertTrue(second.allowed(), "транзиентная недоступность Vault после успешного read не должна рвать уже держащийся бинд");
    }
}
