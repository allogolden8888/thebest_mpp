package uz.mpp.billingreconciliation;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uz.mpp.billingreconciliation.core.Account;
import uz.mpp.billingreconciliation.core.AccountState;
import uz.mpp.billingreconciliation.grpcclient.ExecutionControlClient;
import uz.mpp.billingreconciliation.redisio.BillingRedisClient;
import uz.mpp.billingreconciliation.store.BalanceRecomputer;
import uz.mpp.billingreconciliation.store.ReconciliationAuditStore;
import uz.mpp.platformcontracts.grpc.v1.ApplyOverrideRequest;
import uz.mpp.platformcontracts.grpc.v1.ApplyOverrideResponse;
import uz.mpp.platformcontracts.grpc.v1.ClearOverrideRequest;
import uz.mpp.platformcontracts.grpc.v1.ExecutionControlServiceGrpc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Реальный Postgres + реальный Redis + in-process gRPC fake для Execution
 * Control (тот же паттерн, что {@code ExecutionControlClientTest}) — прямой
 * end-to-end прогон {@link Main#reconcileOne}/{@link Main#reconcileFrozenAccount},
 * не отдельных компонентов по одному. Закрывает "Test quality, cross-cutting"
 * находку кодревью: "No test anywhere exercises the actual Main.java
 * orchestration loops — the freeze-without-unfreeze gap... invisible to the
 * existing unit-test suites."
 */
class MainTest {

    private static final String REDIS_URI = "redis://localhost:6379";
    private static final String JDBC_URL = System.getenv().getOrDefault(
        "BILLING_RECONCILIATION_TEST_JDBC_URL", "jdbc:postgresql://localhost:5432/mpp");

    private Connection pgConnection;
    private DSLContext dsl;
    private BalanceRecomputer recomputer;
    private ReconciliationAuditStore auditStore;

    private RedisClient rawRedisClient;
    private StatefulRedisConnection<String, String> redisConnection;
    private RedisCommands<String, String> redisCommands;
    private BillingRedisClient billingRedisClient;

    private Server grpcServer;
    private ManagedChannel grpcChannel;
    private CapturingExecutionControlService controlService;
    private ExecutionControlClient executionControl;

    private String accountId;

    private static class CapturingExecutionControlService extends ExecutionControlServiceGrpc.ExecutionControlServiceImplBase {
        final CopyOnWriteArrayList<ApplyOverrideRequest> applyRequests = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<ClearOverrideRequest> clearRequests = new CopyOnWriteArrayList<>();
        final AtomicBoolean failApply = new AtomicBoolean(false);

        @Override
        public void applyOverride(ApplyOverrideRequest request, StreamObserver<ApplyOverrideResponse> responseObserver) {
            applyRequests.add(request);
            if (failApply.get()) {
                responseObserver.onError(Status.UNAVAILABLE.withDescription("тестовый сбой Execution Control").asRuntimeException());
                return;
            }
            responseObserver.onNext(ApplyOverrideResponse.newBuilder().setVersion(applyRequests.size()).build());
            responseObserver.onCompleted();
        }

        @Override
        public void clearOverride(ClearOverrideRequest request, StreamObserver<ApplyOverrideResponse> responseObserver) {
            clearRequests.add(request);
            responseObserver.onNext(ApplyOverrideResponse.newBuilder().setVersion(100 + clearRequests.size()).build());
            responseObserver.onCompleted();
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        try {
            pgConnection = DriverManager.getConnection(JDBC_URL);
        } catch (SQLException e) {
            assumeTrue(false, "PostgreSQL недоступен на " + JDBC_URL + " (" + e.getMessage() + ") — пропуск");
            return;
        }
        try {
            rawRedisClient = RedisClient.create(REDIS_URI);
            redisConnection = rawRedisClient.connect();
        } catch (Exception e) {
            assumeTrue(false, "Redis недоступен на " + REDIS_URI + " — пропуск");
            return;
        }

        dsl = DSL.using(pgConnection, SQLDialect.POSTGRES);
        recomputer = new BalanceRecomputer(dsl);
        auditStore = new ReconciliationAuditStore(dsl);

        redisCommands = redisConnection.sync();
        billingRedisClient = new BillingRedisClient(REDIS_URI);

        controlService = new CapturingExecutionControlService();
        String name = "billing-reconciliation-test-" + System.nanoTime();
        grpcServer = InProcessServerBuilder.forName(name).directExecutor().addService(controlService).build().start();
        grpcChannel = InProcessChannelBuilder.forName(name).directExecutor().build();
        executionControl = newClientWithStub(ExecutionControlServiceGrpc.newBlockingStub(grpcChannel));

        accountId = "acc-" + UUID.randomUUID();
        redisCommands.hset("billing:balance:" + accountId, Map.of(
            "balance", "0", "currency", "UZS", "account_state", "ACTIVE", "account_epoch", "0"
        ));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (redisCommands != null) {
            redisCommands.del("billing:balance:" + accountId);
            deleteLedgerRows();
            deleteAuditRows();
        }
        if (billingRedisClient != null) billingRedisClient.close();
        if (redisConnection != null) redisConnection.close();
        if (rawRedisClient != null) rawRedisClient.shutdown();
        if (grpcChannel != null) grpcChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        if (grpcServer != null) grpcServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        if (pgConnection != null) pgConnection.close();
    }

    private void insertLedgerCharge(BigDecimal amountMajorUnits) {
        dsl.insertInto(table("billing.billing_ledger"))
            .columns(field("charge_id", UUID.class), field("account_id", String.class), field("partner_id", String.class),
                field("amount", BigDecimal.class), field("currency", String.class), field("entry_type", String.class), field("source_charge_id", UUID.class))
            .values(UUID.randomUUID(), accountId, "acme", amountMajorUnits, "UZS", "charge", null)
            .execute();
    }

    private void deleteLedgerRows() {
        dsl.deleteFrom(table("billing.billing_ledger")).where(field("account_id", String.class).eq(accountId)).execute();
    }

    private void deleteAuditRows() {
        dsl.deleteFrom(table("billing.reconciliation_audit")).where(field("account_id", String.class).eq(accountId)).execute();
    }

    private long auditRowCount() {
        return dsl.fetchCount(dsl.selectFrom(table("billing.reconciliation_audit")).where(field("account_id", String.class).eq(accountId)));
    }

    private String lastAuditAction() {
        return dsl.select(field("action", String.class))
            .from(table("billing.reconciliation_audit"))
            .where(field("account_id", String.class).eq(accountId))
            .orderBy(field("id", Long.class).desc())
            .limit(1)
            .fetchOne(0, String.class);
    }

    private static ExecutionControlClient newClientWithStub(ExecutionControlServiceGrpc.ExecutionControlServiceBlockingStub stub) throws Exception {
        var ctor = ExecutionControlClient.class.getDeclaredConstructor(ExecutionControlServiceGrpc.ExecutionControlServiceBlockingStub.class);
        ctor.setAccessible(true);
        return ctor.newInstance(stub);
    }

    /**
     * Прямое доказательство исправления CRITICAL находки кодревью #6:
     * FROZEN-аккаунт, чей drift разрешился, реально возвращается в ACTIVE
     * автоматически — без этого фикса раньше ЭТА ветка orchestration
     * (FROZEN-состояние) вообще не существовала.
     */
    @Test
    void frozenAccountWithResolvedDriftIsAutomaticallyUnfrozen() {
        // Redis balance согласован с ledger'ом (delta=0) — учитывается как "не HIGH drift".
        billingRedisClient.freeze(accountId); // epoch 0 -> 1, FROZEN

        Main.reconcileOne(accountId, false, billingRedisClient, recomputer, executionControl, auditStore);

        Account after = billingRedisClient.readAccount(accountId);
        assertEquals(AccountState.ACTIVE, after.state(), "аккаунт должен реально вернуться в ACTIVE — это и есть фикс #6");
        assertEquals(2, after.epoch(), "fenced CAS unfreeze должен продвинуть epoch");
        assertEquals(1, controlService.clearRequests.size(), "triggerUnfreeze должен быть реально вызван на Execution Control");
        assertEquals(accountId + ":BILLING", controlService.clearRequests.get(0).getScopeId());
        assertEquals("UNFREEZE", lastAuditAction());
    }

    /** FROZEN + drift ВСЁ ЕЩЁ HIGH — аккаунт должен остаться FROZEN, не unfreeze. */
    @Test
    void frozenAccountWithPersistentDriftStaysFrozenAndReassertsFreeze() {
        billingRedisClient.freeze(accountId); // epoch -> 1, FROZEN
        insertLedgerCharge(new BigDecimal("500.0000")); // создаёт большую дельту -> HIGH drift относительно balance=0

        Main.reconcileOne(accountId, false, billingRedisClient, recomputer, executionControl, auditStore);

        Account after = billingRedisClient.readAccount(accountId);
        assertEquals(AccountState.FROZEN, after.state(), "drift всё ещё HIGH — не должен размораживаться");
        assertEquals(1, after.epoch(), "epoch не должен меняться, пока аккаунт остаётся FROZEN");
        assertTrue(controlService.applyRequests.size() >= 1, "triggerFreeze должен быть re-asserted, пока drift сохраняется");
        assertEquals(0, controlService.clearRequests.size(), "triggerUnfreeze НЕ должен вызываться");
    }

    /**
     * Прямое доказательство исправления HIGH находки #8: раньше исключение
     * из triggerFreeze пропускало persist(...) целиком, и повторной попытки
     * никогда не было (аккаунт больше не ACTIVE на следующем цикле). Здесь
     * Execution Control гRPC намеренно ломается — Redis всё равно должен
     * реально заморозиться, audit — реально записаться (action=FREEZE_PARTIAL).
     */
    @Test
    void activeAccountFreezesInRedisEvenWhenExecutionControlCallFails() {
        insertLedgerCharge(new BigDecimal("500.0000")); // balance=0 vs ledgerDelta=-50000 -> HIGH drift
        controlService.failApply.set(true);

        Main.reconcileOne(accountId, false, billingRedisClient, recomputer, executionControl, auditStore);

        Account after = billingRedisClient.readAccount(accountId);
        assertEquals(AccountState.FROZEN, after.state(), "Redis freeze не должен зависеть от успеха gRPC-вызова");
        assertEquals(1, after.epoch());
        assertEquals("FREEZE_PARTIAL", lastAuditAction(), "audit должен реально записаться, даже когда triggerFreeze падает — это и есть фикс #8");
    }

    /** driftFreezeExempt (High #7 частичная митигация) — известно-провизированный аккаунт не замораживается автоматически, несмотря на HIGH drift. */
    @Test
    void driftFreezeExemptAccountIsNeverFrozenDespiteHighDrift() {
        insertLedgerCharge(new BigDecimal("500.0000"));

        Main.reconcileOne(accountId, true, billingRedisClient, recomputer, executionControl, auditStore);

        Account after = billingRedisClient.readAccount(accountId);
        assertEquals(AccountState.ACTIVE, after.state(), "exempt-аккаунт не должен замораживаться автоматически");
        assertEquals(0, controlService.applyRequests.size());
        assertEquals("NO_ACTION", lastAuditAction());
    }

    @Test
    void activeAccountWithNoDriftDoesNothing() {
        Main.reconcileOne(accountId, false, billingRedisClient, recomputer, executionControl, auditStore);

        Account after = billingRedisClient.readAccount(accountId);
        assertEquals(AccountState.ACTIVE, after.state());
        assertEquals(0, after.epoch());
        assertEquals(1, auditRowCount());
        assertEquals("NO_ACTION", lastAuditAction());
    }

    @Test
    void unfreezeDoesNotOverwriteRedisBalanceWithGuessedAbsoluteFigure() {
        // High #7: unfreeze не должен переписывать баланс на предположительное
        // число — только сохранить ТЕКУЩИЙ Redis balance, снять FROZEN.
        // balance=50 vs ledgerDelta=0 (нет ledger-строк) -> drift=50 -> LOW
        // (< LOW_THRESHOLD=100) — НЕ HIGH, значит "разрешившийся" drift,
        // должен реально unfreeze'иться (в отличие от HIGH-сценария).
        redisCommands.hset("billing:balance:" + accountId, Map.of("balance", "50"));
        billingRedisClient.freeze(accountId);

        Main.reconcileOne(accountId, false, billingRedisClient, recomputer, executionControl, auditStore);

        Account after = billingRedisClient.readAccount(accountId);
        assertEquals(AccountState.ACTIVE, after.state());
        assertEquals(50, after.balanceMinorUnits(), "unfreeze не должен изменять баланс — только состояние");
    }
}
