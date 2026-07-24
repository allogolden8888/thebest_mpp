package uz.mpp.deliveryreconciliation.store;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Реальный PostgreSQL (brew, локально — та же mpp-база, что остальные
 * сервисы этой сессии, migrations/V010__reconciliation_cases.sql уже
 * применена). Пропускается, если Postgres недоступен.
 */
class ReconciliationStoreTest {

    private Connection connection;
    private ReconciliationStore store;

    @BeforeEach
    void setUp() {
        String url = System.getenv().getOrDefault("DELIVERY_RECONCILIATION_TEST_JDBC_URL", "jdbc:postgresql://localhost:5432/mpp");
        try {
            connection = DriverManager.getConnection(url);
        } catch (SQLException e) {
            assumeTrue(false, "PostgreSQL недоступен на " + url + " (" + e.getMessage() + ") — пропуск");
            return;
        }
        DSLContext dsl = DSL.using(connection, SQLDialect.POSTGRES);
        store = new ReconciliationStore(dsl);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void createThenLoadByMessageIdRoundTrips() {
        UUID messageId = UUID.randomUUID();
        UUID stageExecutionId = UUID.randomUUID();
        Instant deadline = Instant.now().plus(1, ChronoUnit.HOURS);

        ReconciliationCase created = store.create(messageId, stageExecutionId, "beeline_uz", deadline);
        assertEquals("open", created.status());
        assertEquals(messageId, created.messageId());

        Optional<ReconciliationCase> loaded = store.loadByMessageId(messageId);
        assertTrue(loaded.isPresent());
        assertEquals(created.caseId(), loaded.get().caseId());
        assertEquals("beeline_uz", loaded.get().operatorId());
    }

    @Test
    void persistEvidenceUpdatesEvidenceColumn() {
        UUID messageId = UUID.randomUUID();
        ReconciliationCase created = store.create(messageId, UUID.randomUUID(), "ucell_uz", Instant.now().plus(1, ChronoUnit.HOURS));

        store.persistEvidence(created.caseId(), "{\"submit_accepted\":true}");

        ReconciliationCase reloaded = store.loadByMessageId(messageId).orElseThrow();
        assertEquals("{\"submit_accepted\":true}", reloaded.evidenceJson());
    }

    @Test
    void findExpiredOpenCasesReturnsOnlyPastDeadlineOpenCases() {
        UUID expiredMessageId = UUID.randomUUID();
        UUID futureMessageId = UUID.randomUUID();
        store.create(expiredMessageId, UUID.randomUUID(), "beeline_uz", Instant.now().minus(1, ChronoUnit.MINUTES));
        store.create(futureMessageId, UUID.randomUUID(), "beeline_uz", Instant.now().plus(1, ChronoUnit.HOURS));

        var expired = store.findExpiredOpenCases(Instant.now());
        assertTrue(expired.stream().anyMatch(c -> c.messageId().equals(expiredMessageId)));
        assertTrue(expired.stream().noneMatch(c -> c.messageId().equals(futureMessageId)));
    }

    @Test
    void closeCaseSetsStatusAndResolvedAt() {
        UUID messageId = UUID.randomUUID();
        ReconciliationCase created = store.create(messageId, UUID.randomUUID(), "beeline_uz", Instant.now().plus(1, ChronoUnit.HOURS));

        Instant resolvedAt = Instant.now();
        store.closeCase(created.caseId(), "resolved", resolvedAt);

        ReconciliationCase reloaded = store.loadByMessageId(messageId).orElseThrow();
        assertEquals("resolved", reloaded.status());
        assertNotNull(reloaded.resolvedAt());
    }
}