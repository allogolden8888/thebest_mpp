package uz.mpp.billing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.BillingExtension;
import uz.mpp.platformcontracts.common.v1.Outcome;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;
import uz.mpp.platformcontracts.common.v1.StageName;

/**
 * Фаза 5a плана закрытия API-пробелов: прямое доказательство, что
 * account_id теперь резолвится per-message из
 * {@code BillingExtension.partner_id}, не из одной общей константы —
 * два сообщения с разными partner_id обязаны списаться с двух РАЗНЫХ
 * account_id в Billing Redis (реальный, не мок — тот же принцип, что
 * {@link BillingAccountStoreTest}). {@link MockProducer} — единственная
 * часть, которую можно замокать без потери реализма: сама Kafka-доставка
 * здесь не проверяется, только маршрутизация account_id.
 */
class KafkaIoProcessRecordTest {

    private static final String REDIS_URL = System.getenv().getOrDefault("BILLING_ACCOUNT_STORE_TEST_REDIS_URL", "redis://localhost:6379/0");

    private BillingAccountStore accountStore;
    private RedisClient rawClient;
    private BillingService billingService;
    private TariffCache tariffCache;
    private String partnerA;
    private String partnerB;

    @BeforeEach
    void setUp() {
        accountStore = new BillingAccountStore(REDIS_URL);
        rawClient = RedisClient.create(REDIS_URL);
        TariffResolver defaultResolver = TariffResolver.fromFile(
            Path.of("../../config_schemas/examples/billing_tariff.valid.json"));
        billingService = new BillingService(defaultResolver);
        // Ни для partnerA, ни для partnerB ничего не засеяно в Configuration
        // Redis — оба падают на defaultResolver через TariffCache, это
        // ортогонально тому, что здесь реально проверяется (per-partner
        // account_id, не per-partner tariff — тот путь уже покрыт
        // TariffCacheTest отдельно).
        tariffCache = new TariffCache(REDIS_URL, defaultResolver);
        partnerA = "test-partner-a-" + UUID.randomUUID();
        partnerB = "test-partner-b-" + UUID.randomUUID();
    }

    @AfterEach
    void tearDown() {
        try (StatefulRedisConnection<String, String> conn = rawClient.connect()) {
            conn.sync().del("billing:account:" + partnerA, "billing:account:" + partnerB);
        }
        accountStore.close();
        tariffCache.close();
        rawClient.shutdown();
    }

    private static ConsumerRecord<String, byte[]> billingRecord(String messageId, String stageExecutionId, String partnerId, String category, int segmentCount) {
        return billingRecord(messageId, stageExecutionId, partnerId, category, segmentCount, false);
    }

    private static ConsumerRecord<String, byte[]> billingRecord(String messageId, String stageExecutionId, String partnerId, String category, int segmentCount, boolean sandbox) {
        StageExecuteCommand command = StageExecuteCommand.newBuilder()
            .setEventId("evt-" + stageExecutionId)
            .setMessageId(messageId)
            .setStageExecutionId(stageExecutionId)
            .setAttempt(1)
            .setStageName(StageName.STAGE_NAME_BILLING)
            .setSandbox(sandbox)
            .setBilling(BillingExtension.newBuilder()
                .setResolvedOperatorId("beeline")
                .setSegmentCount(segmentCount)
                .setCategory(category)
                .setPartnerId(partnerId)
                .build())
            .build();
        return new ConsumerRecord<>("stage.billing", 0, 0, messageId, command.toByteArray());
    }

    @Test
    void differentPartnerIdsChargeDifferentAccounts() throws Exception {
        MockProducer<String, byte[]> producer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());

        KafkaIo.processRecord(billingRecord("m1", "se1", partnerA, "TRANSACTION", 1), accountStore, billingService, tariffCache, producer);
        KafkaIo.processRecord(billingRecord("m2", "se2", partnerB, "TRANSACTION", 1), accountStore, billingService, tariffCache, producer);

        var accountA = accountStore.peek(partnerA);
        var accountB = accountStore.peek(partnerB);

        assertTrue(accountA.balance() < 0, "partnerA обязан быть реально списан");
        assertTrue(accountB.balance() < 0, "partnerB обязан быть реально списан");
        assertTrue(accountA.processedChargeIds().contains("se1"), "charge se1 обязан числиться на счету partnerA, не partnerB");
        assertTrue(accountB.processedChargeIds().contains("se2"), "charge se2 обязан числиться на счету partnerB, не partnerA");
        assertEquals(2, producer.history().size(), "оба сообщения обязаны опубликовать stage.completed");
    }

    @Test
    void sameChargeIdOnDifferentPartnersDoesNotCollide() throws Exception {
        // charge_id (=stage_execution_id) дедуплицируется ВНУТРИ account_id
        // (billing:account:{account_id} HASH), не глобально — совпадающий
        // stage_execution_id для двух разных партнёров не должен считаться
        // "уже обработанным" на чужом счету.
        MockProducer<String, byte[]> producer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());

        KafkaIo.processRecord(billingRecord("m1", "shared-se", partnerA, "TRANSACTION", 1), accountStore, billingService, tariffCache, producer);
        KafkaIo.processRecord(billingRecord("m2", "shared-se", partnerB, "TRANSACTION", 1), accountStore, billingService, tariffCache, producer);

        assertTrue(accountStore.peek(partnerA).processedChargeIds().contains("shared-se"));
        assertTrue(accountStore.peek(partnerB).processedChargeIds().contains("shared-se"),
            "одинаковый stage_execution_id на другом account_id обязан списаться отдельно, не быть отклонён как дубликат");
        assertTrue(accountStore.peek(partnerA).balance() < 0);
        assertTrue(accountStore.peek(partnerB).balance() < 0);
    }

    /**
     * Фаза 11 плана закрытия API-пробелов: sandbox=true обязан обойти
     * Billing Redis целиком — ни {@code peek}, ни {@code applyChargeAtomically}
     * не должны быть вызваны. Здесь нет Mockito в зависимостях сервиса, а
     * {@link BillingAccountStore} с этой сессии подключается ЖАДНО в
     * конструкторе (see javadoc там — общий connection на весь жизненный
     * цикл сервиса, 1500 TPS push), так что "недостижимый URL" здесь больше
     * не работает как трюк (сам конструктор бы упал раньше processRecord).
     * Доказательство — {@code null}: если бы sandbox-ветка была случайно
     * снята, любое обращение к accountStore немедленно уронило бы тест
     * NullPointerException'ом вместо того, чтобы пройти молча.
     */
    @Test
    void sandboxMessageDoesNotTouchBillingRedis() throws Exception {
        MockProducer<String, byte[]> producer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());

        KafkaIo.processRecord(billingRecord("m1", "se1", partnerA, "TRANSACTION", 1, true), null, billingService, tariffCache, producer);

        assertEquals(1, producer.history().size(), "sandbox-сообщение всё равно обязано опубликовать stage.completed");
        var event = uz.mpp.platformcontracts.common.v1.StageCompletedEvent.parseFrom(producer.history().get(0).value());
        assertEquals(Outcome.OUTCOME_SUCCEEDED, event.getOutcome(), "sandbox-заряд обязан выглядеть как обычный успех");
        assertTrue(event.getSandbox(), "событие обязано нести sandbox=true дальше по пайплайну");
    }

    @Test
    void nonSandboxMessageWithNullAccountStoreThrowsInsteadOfSilentlySucceeding() {
        // Контрольный тест — доказывает, что null действительно был бы
        // использован на не-sandbox пути, не проходит молча по случайности.
        MockProducer<String, byte[]> producer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        assertThrows(NullPointerException.class, () -> KafkaIo.processRecord(
            billingRecord("m1", "se1", partnerA, "TRANSACTION", 1, false), null, billingService, tariffCache, producer));
    }
}
