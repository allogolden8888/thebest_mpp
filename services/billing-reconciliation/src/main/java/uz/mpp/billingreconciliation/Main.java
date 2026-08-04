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
import java.util.Set;
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

        // CODE_REVIEW.md High #7 (частичная митигация, не полный фикс —
        // см. README "Проверено кодревью"): BalanceRecomputer вычисляет
        // ДЕЛЬТУ (compensating - charge), не абсолютный баланс — схема
        // billing.billing_ledger не несёт entry_type для top-up/начального
        // пополнения. Аккаунт, реально провизированный с ненулевым
        // стартовым балансом, показывает перманентный "phantom drift" —
        // никогда не 0, даже если ничего не сломано. Явный allowlist —
        // единственный безопасный (не выдумывающий несуществующий
        // ledger entry_type) способ не замораживать такие аккаунты
        // автоматически, пока реальный фикс схемы не согласован с
        // Главным агентом (владеет billing_ledger).
        Set<String> driftFreezeExemptAccountIds = Set.of(env("RECONCILIATION_DRIFT_FREEZE_EXEMPT_ACCOUNT_IDS", "").split(","));

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> reconcileAll(accountIds, driftFreezeExemptAccountIds, redisClient, recomputer, executionControl, auditStore),
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

    static void reconcileAll(List<String> accountIds, Set<String> driftFreezeExemptAccountIds, BillingRedisClient redisClient,
                                      BalanceRecomputer recomputer, ExecutionControlClient executionControl, ReconciliationAuditStore auditStore) {
        for (String accountId : accountIds) {
            try {
                reconcileOne(accountId, driftFreezeExemptAccountIds.contains(accountId), redisClient, recomputer, executionControl, auditStore);
            } catch (Exception e) {
                System.err.println("reconciliation failed for account_id=" + accountId + ": " + e.getMessage());
            }
        }
    }

    /**
     * CODE_REVIEW.md Critical #6 + High #8 — раньше обрабатывала ТОЛЬКО
     * ACTIVE-аккаунты (freeze при HIGH drift); FROZEN-аккаунт никогда не
     * пересматривался — ни одного пути назад в ACTIVE, каждый реальный
     * drift-инцидент навсегда сажал аккаунт "на прикол". Теперь оба
     * состояния явно обрабатываются: ACTIVE -&gt; (freeze | no-op), FROZEN
     * -&gt; ({@link #reconcileFrozenAccount} — re-assert freeze при
     * сохраняющемся drift, ИЛИ recompute-then-unfreeze при разрешившемся).
     */
    static void reconcileOne(String accountId, boolean driftFreezeExempt, BillingRedisClient redisClient, BalanceRecomputer recomputer,
                                      ExecutionControlClient executionControl, ReconciliationAuditStore auditStore) {
        Account account = redisClient.readAccount(accountId);
        long ledgerDelta = recomputer.recomputeDeltaMinorUnits(accountId);

        // compute_drift: сравнение изменения Redis-баланса с дельтой ledger'а
        // за то же окно потребовало бы снапшота "баланс на начало окна" —
        // здесь сверяется, что текущий Redis balance согласуется с
        // накопленной ledger-дельтой (см. store/BalanceRecomputer докстринг
        // "Открытый вопрос" про отсутствие top-up entry_type, и
        // CODE_REVIEW.md High #7 — README "Проверено кодревью").
        DriftReport report = new DriftReport(accountId, account.balanceMinorUnits(), ledgerDelta);
        DriftClassifier.Severity severity = DriftClassifier.classify(report);
        String partnerId = accountId; // TODO: резолв account_id -> partner_id не специфицирован, см. README

        if (account.state() == AccountState.FROZEN) {
            reconcileFrozenAccount(accountId, partnerId, severity, report, account, redisClient, executionControl, auditStore);
            return;
        }

        if (driftFreezeExempt || !DriftClassifier.shouldFreeze(severity)) {
            auditStore.persist(accountId, report.driftMinorUnits(), severity.name(), "NO_ACTION", null, account.epoch(), account.epoch(), Instant.now());
            return;
        }

        // freeze() в BillingRedisClient идемпотентен (WATCH/MULTI/EXEC,
        // ретраит на конфликт) — на ACTIVE-аккаунте гарантированно
        // применяется и продвигает epoch на +1.
        redisClient.freeze(accountId);
        long epochAfter = account.epoch() + 1;

        // CODE_REVIEW.md High #8: раньше исключение из triggerFreeze
        // (gRPC на Execution Control) пропускало persist(...) целиком — на
        // следующем цикле account.state() уже не ACTIVE, эта ветка больше
        // никогда не выполнялась, triggerFreeze никогда не повторялся, и
        // ни единой аудит-записи не оставалось о том, что что-то пошло не
        // так. Теперь: audit персистится ВСЕГДА (успех или
        // FREEZE_PARTIAL), а reconcileFrozenAccount ниже реально повторяет
        // triggerFreeze на КАЖДОМ следующем цикле, пока drift остаётся
        // HIGH — провалившийся gRPC-вызов не теряется навсегда, а
        // естественно повторяется максимум через 60с (интервал
        // reconciliation).
        String action = "FREEZE";
        try {
            executionControl.triggerFreeze(partnerId, "billing drift severity " + severity);
        } catch (Exception e) {
            System.err.println("trigger_freeze (Execution Control) failed for account_id=" + accountId
                + " — Redis уже FROZEN, gRPC-вызов будет автоматически повторён на следующем цикле, пока drift сохраняется HIGH: " + e.getMessage());
            action = "FREEZE_PARTIAL";
        }
        auditStore.persist(accountId, report.driftMinorUnits(), severity.name(), action, null, account.epoch(), epochAfter, Instant.now());
    }

    static void reconcileFrozenAccount(String accountId, String partnerId, DriftClassifier.Severity severity, DriftReport report,
                                                 Account account, BillingRedisClient redisClient, ExecutionControlClient executionControl,
                                                 ReconciliationAuditStore auditStore) {
        if (DriftClassifier.shouldFreeze(severity)) {
            // Drift всё ещё HIGH — аккаунт остаётся FROZEN, но re-assert
            // triggerFreeze (идемпотентно на стороне Execution Control,
            // тот же ApplyOverride) — см. докстринг метода выше про #8.
            try {
                executionControl.triggerFreeze(partnerId, "billing drift severity " + severity + " (re-asserted, account still frozen)");
            } catch (Exception e) {
                System.err.println("trigger_freeze re-assert failed for account_id=" + accountId + ": " + e.getMessage());
            }
            auditStore.persist(accountId, report.driftMinorUnits(), severity.name(), "NO_ACTION", null, account.epoch(), account.epoch(), Instant.now());
            return;
        }

        // CODE_REVIEW.md Critical #6: recompute-then-unfreeze. Намеренно НЕ
        // форсируем произвольный "recomputed balance" в Redis — в текущей
        // ledger-схеме нет достоверного источника АБСОЛЮТНОГО баланса,
        // которым было бы безопасно перезаписать Redis (см. High #7 выше).
        // "Drift разрешился" здесь означает именно "текущий Redis-баланс
        // снова согласуется с ledger'ом" — applyFencedUnfreeze сохраняет
        // ТЕКУЩИЙ Redis-баланс как есть (передаём account.balanceMinorUnits()
        // без изменения), не переписывает его на предположительное число.
        boolean unfrozen = redisClient.applyFencedUnfreeze(accountId, account.balanceMinorUnits(), account.epoch());
        if (!unfrozen) {
            // CAS-конфликт — epoch изменился между readAccount() (в
            // reconcileOne) и этим вызовом (например конкурентная реплика
            // reconciliation — Medium #10 в README, не устранено в этом
            // проходе). Безопасно: просто повторим на следующем цикле, ничего
            // не потеряно.
            auditStore.persist(accountId, report.driftMinorUnits(), severity.name(), "UNFREEZE_CONFLICT", null, account.epoch(), account.epoch(), Instant.now());
            return;
        }

        long epochAfter = account.epoch() + 1;
        try {
            executionControl.triggerUnfreeze(partnerId);
        } catch (Exception e) {
            // Redis уже ACTIVE (fenced CAS выше уже применён) — если ИМЕННО
            // этот gRPC вызов проваливается, следующий цикл увидит
            // state=ACTIVE (не FROZEN) и не пойдёт в эту ветку снова —
            // асимметрия с triggerFreeze re-assert выше (там retry
            // естественный, потому что состояние остаётся FROZEN).
            // Остаточный риск задокументирован в README, не решается в этом
            // проходе — Redis (source of truth для billing charge fast-path)
            // уже корректно ACTIVE, только Execution Control override может
            // на короткое время отстать до ручного вмешательства.
            System.err.println("trigger_unfreeze failed for account_id=" + accountId + " after successful Redis unfreeze: " + e.getMessage());
        }
        auditStore.persist(accountId, report.driftMinorUnits(), severity.name(), "UNFREEZE", account.balanceMinorUnits(), account.epoch(), epochAfter, Instant.now());
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
