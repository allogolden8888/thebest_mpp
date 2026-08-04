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

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Реальный PostgreSQL (brew, локально, migrations/V008 уже применена). */
class LedgerStoreTest {

    private Connection connection;
    private LedgerStore store;

    @BeforeEach
    void setUp() {
        String url = System.getenv().getOrDefault("BILLING_LEDGER_WRITER_TEST_JDBC_URL", "jdbc:postgresql://localhost:5432/mpp");
        try {
            connection = DriverManager.getConnection(url);
        } catch (SQLException e) {
            assumeTrue(false, "PostgreSQL недоступен на " + url + " (" + e.getMessage() + ") — пропуск");
            return;
        }
        DSLContext dsl = DSL.using(connection, SQLDialect.POSTGRES);
        store = new LedgerStore(dsl);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void insertNewChargeSucceeds() {
        UUID chargeId = UUID.randomUUID();
        LedgerEntry entry = new LedgerEntry(chargeId, "acc-" + chargeId, "acme", new BigDecimal("150.0000"), "UZS", "charge", null);

        boolean inserted = store.insert(entry);
        assertTrue(inserted);
        assertTrue(store.exists(chargeId));
    }

    @Test
    void duplicateChargeIdIsIdempotentNoOp() {
        UUID chargeId = UUID.randomUUID();
        LedgerEntry entry = new LedgerEntry(chargeId, "acc-" + chargeId, "acme", new BigDecimal("150.0000"), "UZS", "charge", null);

        boolean firstInsert = store.insert(entry);
        boolean secondInsert = store.insert(entry);

        assertTrue(firstInsert);
        assertFalse(secondInsert, "повторная вставка с тем же charge_id должна быть no-op, не ошибкой");
    }

    @Test
    void compensatingEntryWithSourceChargeIdInsertsSuccessfully() {
        UUID originalChargeId = UUID.randomUUID();
        UUID compensatingChargeId = UUID.randomUUID();
        store.insert(new LedgerEntry(originalChargeId, "acc-x", "acme", new BigDecimal("100.0000"), "UZS", "charge", null));

        boolean inserted = store.insert(new LedgerEntry(compensatingChargeId, "acc-x", "acme", new BigDecimal("100.0000"), "UZS", "compensating", originalChargeId));
        assertTrue(inserted);
    }
}
