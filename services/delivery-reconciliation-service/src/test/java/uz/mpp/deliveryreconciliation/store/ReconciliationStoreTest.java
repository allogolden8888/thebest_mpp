package uz.mpp.deliveryreconciliation.store;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uz.mpp.deliveryreconciliation.core.Evidence;
import uz.mpp.deliveryreconciliation.core.EvidenceCodec;

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
        // Колонка — JSONB (migrations/V010__reconciliation_cases.sql):
        // PostgreSQL нормализует whitespace при хранении/чтении (напр.
        // добавляет пробел после ":"), поэтому побайтовое сравнение сырой
        // строки хрупко — сравниваем через EvidenceCodec, тот же уровень
        // абстракции, что использует Main (CODE_REVIEW.md #11).
        assertEquals(Evidence.empty().withSubmitAccepted(), EvidenceCodec.decode(reloaded.evidenceJson()));
    }

    @Test
    void persistEvidenceRoundTripsAllThreeFieldsThroughRealJsonb() {
        UUID messageId = UUID.randomUUID();
        ReconciliationCase created = store.create(messageId, UUID.randomUUID(), "ucell_uz", Instant.now().plus(1, ChronoUnit.HOURS));
        Evidence evidence = Evidence.empty()
            .withSubmitAccepted()
            .withDeliveryStatus(Evidence.DeliveryOutcome.FAILURE)
            .withQuerySm(Evidence.QuerySmOutcome.INCONCLUSIVE);

        store.persistEvidence(created.caseId(), EvidenceCodec.encode(evidence));

        ReconciliationCase reloaded = store.loadByMessageId(messageId).orElseThrow();
        assertEquals(evidence, EvidenceCodec.decode(reloaded.evidenceJson()));
    }

    @Test
    void createInitializesEvidenceToEmptyJsonObject() {
        UUID messageId = UUID.randomUUID();
        store.create(messageId, UUID.randomUUID(), "beeline_uz", Instant.now().plus(1, ChronoUnit.HOURS));

        ReconciliationCase reloaded = store.loadByMessageId(messageId).orElseThrow();
        assertEquals(Evidence.empty(), EvidenceCodec.decode(reloaded.evidenceJson()));
    }

    @Test
    void findExpiredOpenCasesReturnsOnlyPastDeadlineOpenCases() {
        UUID expiredMessageId = UUID.randomUUID();
        UUID futureMessageId = UUID.randomUUID();
        store.create(expiredMessageId, UUID.randomUUID(), "beeline_uz", Instant.now().minus(1, ChronoUnit.MINUTES));
        store.create(futureMessageId, UUID.randomUUID(), "beeline_uz", Instant.now().plus(1, ChronoUnit.HOURS));

        // Лимит намеренно взят заведомо больше любого реального бэклога:
        // findExpiredOpenCases отдаёт САМЫЕ ПРОСРОЧЕННЫЕ первыми, а тест гоняется
        // против общей живой mpp-базы, где уже лежат чужие просроченные case'ы
        // старше только что созданного здесь. С маленьким лимитом тест проверял
        // бы не предикат выборки, а всего лишь то, что бэклог короче лимита.
        var expired = store.findExpiredOpenCases(Instant.now(), Integer.MAX_VALUE);
        assertTrue(expired.stream().anyMatch(c -> c.messageId().equals(expiredMessageId)));
        assertTrue(expired.stream().noneMatch(c -> c.messageId().equals(futureMessageId)));
    }

    @Test
    void findExpiredOpenCasesRespectsLimit() {
        for (int i = 0; i < 3; i++) {
            store.create(UUID.randomUUID(), UUID.randomUUID(), "beeline_uz", Instant.now().minus(1, ChronoUnit.MINUTES));
        }

        assertEquals(2, store.findExpiredOpenCases(Instant.now(), 2).size(),
            "неограниченная выборка при бэклоге грузит в heap весь бэклог — LIMIT обязателен");
    }

    @Test
    void closeCasesClosesEveryCaseOfTheBatchInOneStatement() {
        UUID firstMessageId = UUID.randomUUID();
        UUID secondMessageId = UUID.randomUUID();
        UUID untouchedMessageId = UUID.randomUUID();
        ReconciliationCase first = store.create(firstMessageId, UUID.randomUUID(), "beeline_uz", Instant.now().minus(1, ChronoUnit.MINUTES));
        ReconciliationCase second = store.create(secondMessageId, UUID.randomUUID(), "beeline_uz", Instant.now().minus(1, ChronoUnit.MINUTES));
        store.create(untouchedMessageId, UUID.randomUUID(), "beeline_uz", Instant.now().minus(1, ChronoUnit.MINUTES));

        store.closeCases(java.util.List.of(first.caseId(), second.caseId()), "resolved", Instant.now());

        assertEquals("resolved", store.loadByMessageId(firstMessageId).orElseThrow().status());
        assertEquals("resolved", store.loadByMessageId(secondMessageId).orElseThrow().status());
        assertNotNull(store.loadByMessageId(secondMessageId).orElseThrow().resolvedAt());
        assertEquals("open", store.loadByMessageId(untouchedMessageId).orElseThrow().status(),
            "closeCases не должен трогать case'ы вне переданного списка");
    }

    @Test
    void closeCasesOnEmptyListIsNoOp() {
        UUID messageId = UUID.randomUUID();
        store.create(messageId, UUID.randomUUID(), "beeline_uz", Instant.now().minus(1, ChronoUnit.MINUTES));

        store.closeCases(java.util.List.of(), "resolved", Instant.now());

        assertEquals("open", store.loadByMessageId(messageId).orElseThrow().status(),
            "пустой батч (все публикации провалились) не должен закрывать вообще ничего");
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