package uz.mpp.billingledgerwriter.store;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Реальный PostgreSQL (brew, локально) — прямое доказательство исправления
 * CRITICAL находки кодревью #3: раньше единственный {@link Connection}
 * создавался один раз и переиспользовался НАВСЕГДА, даже мёртвый. Здесь
 * соединение принудительно рвётся (тот же наблюдаемый эффект, что реальный
 * failover Postgres — {@code isValid()} становится false), и
 * {@link ReconnectingConnectionProvider} должен прозрачно восстановиться, а
 * не продолжать отдавать сломанный объект.
 */
class ReconnectingConnectionProviderTest {

    private static final String URL = System.getenv().getOrDefault(
        "BILLING_LEDGER_WRITER_TEST_JDBC_URL", "jdbc:postgresql://localhost:5432/mpp");

    private ReconnectingConnectionProvider provider;

    @BeforeEach
    void setUp() {
        try (Connection probe = DriverManager.getConnection(URL)) {
            // просто проверяем доступность Postgres перед тем, как создавать provider
        } catch (SQLException e) {
            assumeTrue(false, "PostgreSQL недоступен на " + URL + " (" + e.getMessage() + ") — пропуск");
            return;
        }
        try {
            provider = new ReconnectingConnectionProvider(URL);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tearDown() {
        if (provider != null) {
            provider.close();
        }
    }

    @Test
    void acquireReturnsSameConnectionWhileHealthy() throws SQLException {
        Connection first = provider.acquire();
        Connection second = provider.acquire();
        assertTrue(first == second, "здоровое соединение не должно пересоздаваться на каждый acquire()");
        assertTrue(first.isValid(2));
    }

    /**
     * Прямое доказательство исправления: раньше ЭТОТ сценарий (соединение
     * разорвано между двумя acquire) означал, что КАЖДЫЙ следующий insert
     * падает одинаково — навсегда, до ручного рестарта пода.
     */
    @Test
    void acquireReconnectsTransparentlyAfterConnectionIsBroken() throws SQLException {
        Connection broken = provider.acquire();
        broken.close(); // симулирует разрыв TCP-соединения к Postgres (failover, сетевой блип)
        assertTrue(!broken.isValid(2), "тестовая предпосылка: закрытое соединение должно быть невалидным");

        Connection reconnected = provider.acquire();
        assertNotSame(broken, reconnected, "acquire() должен вернуть НОВОЕ соединение после того, как старое сломалось");
        assertTrue(reconnected.isValid(2), "новое соединение должно быть реально рабочим");
    }

    @Test
    void ledgerStoreInsertSucceedsAfterUnderlyingConnectionWasBrokenAndReconnected() {
        DSLContext dsl = DSL.using(provider, SQLDialect.POSTGRES);
        LedgerStore store = new LedgerStore(dsl);

        try {
            provider.acquire().close(); // ломаем до первого реального использования store
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        UUID chargeId = UUID.randomUUID();
        LedgerEntry entry = new LedgerEntry(chargeId, "acc-" + chargeId, "acme", new BigDecimal("150.0000"), "UZS", "charge", null);

        boolean inserted = store.insert(entry);
        assertTrue(inserted, "insert через LedgerStore должен реально пройти после прозрачного reconnect, не упасть на мёртвом соединении");
        assertTrue(store.exists(chargeId));
    }
}
