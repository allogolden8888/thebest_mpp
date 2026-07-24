package uz.mpp.billingreconciliation;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import uz.mpp.billingreconciliation.core.Account;
import uz.mpp.billingreconciliation.core.AccountState;
import uz.mpp.billingreconciliation.core.DriftClassifier;
import uz.mpp.billingreconciliation.core.DriftReport;
import uz.mpp.billingreconciliation.grpcclient.ExecutionControlClient;
import uz.mpp.billingreconciliation.health.HealthServer;
import uz.mpp.billingreconciliation.redisio.BillingRedisClient;
import uz.mpp.billingreconciliation.store.BalanceRecomputer;
import uz.mpp.billingreconciliation.store.ReconciliationAuditStore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Billing Reconciliation (services_specifictaion.md §6.3): сравнение Redis
 * balance и PostgreSQL ledger, freeze аккаунта, fenced recovery.
 *
 * <p><b>Отклонение от LLD-стека</b>: собран без Micronaut, тот же выбор и та
 * же причина, что services/delivery-reconciliation-service (см. его README).
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        HealthServer health = new HealthServer();
        health.start();

        Connection connection = DriverManager.getConnection(buildJdbcUrl());
        DSLContext dsl = DSL.using(connection, SQLDialect.POSTGRES);
        BalanceRecomputer recomputer = new BalanceRecomputer(dsl);
        ReconciliationAuditStore auditStore = new ReconciliationAuditStore(dsl);

        String redisUri = "redis://" + env("REDIS_BILLING_HOST", "localhost") + ":" + env("REDIS_BILLING_PORT", "6379");
        BillingRedisClient redisClient = new BillingRedisClient(redisUri);

        ExecutionControlClient executionControl = new ExecutionControlClient(
            env("EXECUTION_CONTROL_HOST", "execution-control-service.mpp.svc"),
            Integer.parseInt(env("EXECUTION_CONTROL_PORT", "9000")));

        // Список аккаунтов для периодической сверки — не существует ни
        // одного метода "перечислить все billing-аккаунты" в этом срезе LLD;
        // здесь заглушка через env (CSV), реальный источник (например,
        // SCAN по billing:balance:* в Redis, или отдельная таблица счетов
        // в PostgreSQL) не определён ни в одном документе, см. README
        // "Открытый вопрос".
        List<String> accountIds = List.of(env("RECONCILIATION_ACCOUNT_IDS", "").split(",")).stream()
            .filter(s -> !s.isBlank()).toList();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> reconcileAll(accountIds, redisClient, recomputer, executionControl, auditStore),
            0, 60, TimeUnit.SECONDS);

        health.setReady(true);
        System.out.println("billing-reconciliation готов");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduler.shutdown();
            redisClient.close();
            executionControl.close();
            health.stop();
        }));

        Thread.currentThread().join();
    }

    private static void reconcileAll(List<String> accountIds, BillingRedisClient redisClient, BalanceRecomputer recomputer,
                                      ExecutionControlClient executionControl, ReconciliationAuditStore auditStore) {
        for (String accountId : accountIds) {
            try {
                reconcileOne(accountId, redisClient, recomputer, executionControl, auditStore);
            } catch (Exception e) {
                System.err.println("reconciliation failed for account_id=" + accountId + ": " + e.getMessage());
            }
        }
    }

    private static void reconcileOne(String accountId, BillingRedisClient redisClient, BalanceRecomputer recomputer,
                                      ExecutionControlClient executionControl, ReconciliationAuditStore auditStore) {
        Account account = redisClient.readAccount(accountId);
        long ledgerDelta = recomputer.recomputeDeltaMinorUnits(accountId);

        // compute_drift: сравнение изменения Redis-баланса с дельтой ledger'а
        // за то же окно потребовало бы снапшота "баланс на начало окна" —
        // здесь сверяется, что текущий Redis balance согласуется с
        // накопленной ledger-дельтой (см. store/BalanceRecomputer докстринг
        // "Открытый вопрос" про отсутствие top-up entry_type).
        DriftReport report = new DriftReport(accountId, account.balanceMinorUnits(), ledgerDelta);
        DriftClassifier.Severity severity = DriftClassifier.classify(report);

        String action = "NO_ACTION";
        Long recomputedBalance = null;
        long epochBefore = account.epoch();
        long epochAfter = epochBefore;

        if (DriftClassifier.shouldFreeze(severity) && account.state() == AccountState.ACTIVE) {
            redisClient.freeze(accountId);
            String partnerId = accountId; // TODO: резолв account_id -> partner_id не специфицирован, см. README
            executionControl.triggerFreeze(partnerId, "billing drift severity " + severity);
            action = "FREEZE";
            epochAfter = epochBefore + 1;
        }

        auditStore.persist(accountId, report.driftMinorUnits(), severity.name(), action, recomputedBalance, epochBefore, epochAfter, Instant.now());
    }

    private static String buildJdbcUrl() {
        String host = env("POSTGRES_HOST", "localhost");
        String port = env("POSTGRES_PORT", "5432");
        String db = env("POSTGRES_DB", "mpp");
        String user = env("POSTGRES_USER", "");
        String password = env("POSTGRES_PASSWORD", "");
        if (user.isEmpty()) {
            return "jdbc:postgresql://" + host + ":" + port + "/" + db;
        }
        return "jdbc:postgresql://" + host + ":" + port + "/" + db + "?user=" + user + "&password=" + password;
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }
}