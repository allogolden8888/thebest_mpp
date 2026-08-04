package uz.mpp.billingreconciliation.store;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Реальный PostgreSQL (brew, локально, migrations/V008 уже применена). */
class BalanceRecomputerTest {

    private Connection connection;
    private DSLContext dsl;
    private BalanceRecomputer recomputer;

    @BeforeEach
    void setUp() {
        String url = System.getenv().getOrDefault("BILLING_RECONCILIATION_TEST_JDBC_URL", "jdbc:postgresql://localhost:5432/mpp");
        try {
            connection = DriverManager.getConnection(url);
        } catch (SQLException e) {
            assumeTrue(false, "PostgreSQL недоступен на " + url + " (" + e.getMessage() + ") — пропуск");
            return;
        }
        dsl = DSL.using(connection, SQLDialect.POSTGRES);
        recomputer = new BalanceRecomputer(dsl);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    private void insertLedgerRow(UUID chargeId, String accountId, String amount, String entryType, UUID sourceChargeId) {
        dsl.execute("INSERT INTO billing.billing_ledger (charge_id, account_id, partner_id, amount, currency, entry_type, source_charge_id) VALUES (?, ?, 'acme', ?, 'UZS', ?, ?)",
            chargeId, accountId, new java.math.BigDecimal(amount), entryType, sourceChargeId);
    }

    @Test
    void recomputesNegativeDeltaForChargesOnly() {
        String accountId = "acc-" + UUID.randomUUID();
        insertLedgerRow(UUID.randomUUID(), accountId, "150.0000", "charge", null);
        insertLedgerRow(UUID.randomUUID(), accountId, "50.0000", "charge", null);

        long delta = recomputer.recomputeDeltaMinorUnits(accountId);
        assertEquals(-20000, delta, "200.00 UZS списано -> -20000 тийин дельта");
    }

    @Test
    void compensatingEntryOffsetsCharge() {
        String accountId = "acc-" + UUID.randomUUID();
        UUID chargeId = UUID.randomUUID();
        insertLedgerRow(chargeId, accountId, "150.0000", "charge", null);
        insertLedgerRow(UUID.randomUUID(), accountId, "150.0000", "compensating", chargeId);

        long delta = recomputer.recomputeDeltaMinorUnits(accountId);
        assertEquals(0, delta, "compensating полностью компенсирует charge -> дельта 0");
    }

    @Test
    void accountWithNoLedgerRowsHasZeroDelta() {
        long delta = recomputer.recomputeDeltaMinorUnits("acc-" + UUID.randomUUID());
        assertEquals(0, delta);
    }
}
