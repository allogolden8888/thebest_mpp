package uz.mpp.deliveryreconciliation;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import uz.mpp.deliveryreconciliation.core.Evidence;
import uz.mpp.deliveryreconciliation.core.EvidenceCodec;
import uz.mpp.deliveryreconciliation.kafkaio.StageCompletedPublisher;
import uz.mpp.deliveryreconciliation.store.CaseStore;
import uz.mpp.deliveryreconciliation.store.ReconciliationCase;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Тесты на две гонки, из-за которых на живом стенде 16 187 сообщений получили
 * финальный RECONCILIATION_OUTCOME_CONFIRMED_NOT_SUBMITTED, имея при этом
 * DELIVERY = SUCCEEDED (замерено в ClickHouse, analytics.stage_events;
 * перекрёстный запрос показал, что таковы ВСЕ 16 187 без исключения).
 *
 * <p>Обе гонки — про ПОРЯДОК обращений к хранилищу, поэтому проверяются на
 * фейке {@link CaseStore}, а не на живом PostgreSQL: нужное чередование
 * (свидетельство раньше case'а) на реальной БД по заказу не воспроизводится,
 * а здесь оно задаётся явно. SQL-реализация {@code ReconciliationStore}
 * отдельно покрыта {@code ReconciliationStoreTest} против настоящей БД.
 *
 * <p>Третий тест сторожит, что починка не сломала исходное поведение: case,
 * до которого свидетельство так и не дошло, обязан по-прежнему закрываться по
 * дедлайну.
 */
class ReconciliationRaceTest {

    private static StageCompletedPublisher publisher(MockProducer<String, byte[]> mock) {
        return new StageCompletedPublisher(mock);
    }

    private static MockProducer<String, byte[]> mockProducer() {
        return new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer());
    }

    /**
     * Гонка №1, главная. Свидетельство (DLR «доставлено») приходит РАНЬШЕ, чем
     * pipeline-engine успел создать case. Раньше оно молча выбрасывалось
     * ({@code loadByMessageId(...).ifPresent(...)} — нет case'а, нет и
     * получателя), и через две минуты sweep закрывал case по дедлайну как
     * «не отправлено».
     */
    @Test
    void свидетельствоПришедшееДоCaseНеТеряется() {
        FakeCaseStore store = new FakeCaseStore();
        MockProducer<String, byte[]> mock = mockProducer();
        UUID messageId = UUID.randomUUID();

        // 1. DLR обгоняет создание case'а.
        Main.mergeEvidence(store, publisher(mock), messageId,
            Evidence.empty().withDeliveryStatus(Evidence.DeliveryOutcome.SUCCESS));

        assertTrue(store.loadByMessageId(messageId).isEmpty(), "case ещё не создан");
        assertTrue(store.loadEarlyEvidence(messageId).isPresent(),
            "свидетельство обязано переждать отсутствие case'а в early_evidence, а не пропасть");

        // 2. Теперь приходит stage.delivery-reconciliation и создаёт case.
        Main.openCase(store, publisher(mock), messageId, UUID.randomUUID(), "op-1");

        ReconciliationCase caze = store.loadByMessageId(messageId).orElseThrow();
        assertEquals("resolved", caze.status(),
            "case обязан закрыться сразу подхваченным свидетельством, а не ждать дедлайна");
        assertEquals(1, mock.history().size(), "опубликован ровно один stage.completed");
        assertTrue(store.loadEarlyEvidence(messageId).isEmpty(),
            "влитая строка early_evidence удаляется, иначе таблица растёт без границы");
    }

    /**
     * Гонка №2. Case уже есть, дедлайн далеко впереди, приходит терминальный
     * DLR. Раньше sweep выбирал только ПРОСРОЧЕННЫЕ case'ы, поэтому даже
     * дошедшее свидетельство не закрывало case немедленно — сообщение висело
     * все две минуты окна (замерено: минимум DELIVERY->RECON по 80 966
     * сообщениям — 120 044 мс, раньше дедлайна не закрылся НИ ОДИН case).
     */
    @Test
    void терминальныйDlrЗакрываетCaseНеДожидаясьДедлайна() {
        FakeCaseStore store = new FakeCaseStore();
        MockProducer<String, byte[]> mock = mockProducer();
        UUID messageId = UUID.randomUUID();

        // Дедлайн относителен реального времени: фиксированная дата
        // сделала тест необратимо красным после наступления этой даты.
        store.create(messageId, UUID.randomUUID(), "op-1", Instant.now().plusSeconds(3600));
        assertTrue(store.findExpiredOpenCases(Instant.now(), 10).isEmpty(),
            "предусловие теста: по дедлайну этот case сейчас не выбирается");

        Main.mergeEvidence(store, publisher(mock), messageId,
            Evidence.empty().withDeliveryStatus(Evidence.DeliveryOutcome.SUCCESS));

        assertEquals("resolved", store.loadByMessageId(messageId).orElseThrow().status());
        assertEquals(1, mock.history().size());
    }

    /**
     * Регрессия на исходное поведение: свидетельства нет вовсе. Такой case
     * закрывать досрочно нечем — он обязан дожить до дедлайна и уйти обычным
     * sweep'ом. Если бы починка закрывала case'ы «на всякий случай», она
     * заменила бы одну порчу данных другой.
     */
    @Test
    void безСвидетельстваCaseПоПрежнемуЖдётДедлайна() {
        FakeCaseStore store = new FakeCaseStore();
        MockProducer<String, byte[]> mock = mockProducer();
        UUID messageId = UUID.randomUUID();

        Main.openCase(store, publisher(mock), messageId, UUID.randomUUID(), "op-1");

        assertEquals("open", store.loadByMessageId(messageId).orElseThrow().status(),
            "нечем резолвить — case остаётся открытым");
        assertTrue(mock.history().isEmpty(), "ничего публиковать ещё нельзя");
    }

    /**
     * Защитная проверка в {@code sweepBatch} не должна давать ложных
     * срабатываний на нормальном пути: submit_accepted без DLR до дедлайна —
     * это штатный CONFIRMED_SUBMITTED, а не порча данных.
     */
    @Test
    void submitAcceptedБезDlrНеЗакрываетCaseДосрочно() {
        FakeCaseStore store = new FakeCaseStore();
        MockProducer<String, byte[]> mock = mockProducer();
        UUID messageId = UUID.randomUUID();

        store.create(messageId, UUID.randomUUID(), "op-1", Instant.now().plusSeconds(3600));
        Main.mergeEvidence(store, publisher(mock), messageId, Evidence.empty().withSubmitAccepted());

        assertEquals("open", store.loadByMessageId(messageId).orElseThrow().status(),
            "DLR сильнее submit_accepted и ещё может прийти — закрывать рано");
        assertTrue(mock.history().isEmpty());
        assertTrue(store.loadByMessageId(messageId).orElseThrow().evidenceJson().contains("true"),
            "но само свидетельство обязано быть записано в case");
    }

    /** Потокобезопасность не проверяется: тесты однопоточные, гонка задаётся порядком вызовов. */
    private static final class FakeCaseStore implements CaseStore {

        private final Map<UUID, ReconciliationCase> byMessageId = new HashMap<>();
        private final Map<UUID, UUID> messageIdByCaseId = new HashMap<>();
        private final Map<UUID, Evidence> earlyEvidence = new HashMap<>();

        @Override
        public List<ReconciliationCase> findExpiredOpenCases(Instant now, int limit) {
            List<ReconciliationCase> out = new ArrayList<>();
            for (ReconciliationCase c : byMessageId.values()) {
                if ("open".equals(c.status()) && c.deadlineAt().isBefore(now) && out.size() < limit) {
                    out.add(c);
                }
            }
            return out;
        }

        @Override
        public Optional<ReconciliationCase> loadByMessageId(UUID messageId) {
            return Optional.ofNullable(byMessageId.get(messageId));
        }

        @Override
        public ReconciliationCase create(UUID messageId, UUID stageExecutionId, String operatorId, Instant deadlineAt) {
            // ON CONFLICT (message_id) DO NOTHING — повторный вызов отдаёт
            // существующий case, как реальный ReconciliationStore.
            ReconciliationCase existing = byMessageId.get(messageId);
            if (existing != null) {
                return existing;
            }
            UUID caseId = UUID.randomUUID();
            ReconciliationCase created = new ReconciliationCase(
                caseId, messageId, stageExecutionId, operatorId, "open",
                Instant.now(), null, deadlineAt, "{}");
            byMessageId.put(messageId, created);
            messageIdByCaseId.put(caseId, messageId);
            return created;
        }

        @Override
        public void persistEvidence(UUID caseId, String evidenceJson) {
            UUID messageId = messageIdByCaseId.get(caseId);
            ReconciliationCase c = byMessageId.get(messageId);
            byMessageId.put(messageId, c.withEvidenceJson(evidenceJson));
        }

        @Override
        public void closeCases(List<UUID> caseIds, String finalStatus, Instant resolvedAt) {
            for (UUID caseId : caseIds) {
                UUID messageId = messageIdByCaseId.get(caseId);
                ReconciliationCase c = byMessageId.get(messageId);
                byMessageId.put(messageId, new ReconciliationCase(
                    c.caseId(), c.messageId(), c.stageExecutionId(), c.operatorId(),
                    finalStatus, c.openedAt(), resolvedAt, c.deadlineAt(), c.evidenceJson()));
            }
        }

        @Override
        public void recordEarlyEvidence(UUID messageId, Evidence delta) {
            earlyEvidence.merge(messageId, delta, Evidence::merge);
        }

        @Override
        public Optional<Evidence> loadEarlyEvidence(UUID messageId) {
            return Optional.ofNullable(earlyEvidence.get(messageId));
        }

        @Override
        public void deleteEarlyEvidence(UUID messageId) {
            earlyEvidence.remove(messageId);
        }

        @Override
        public int purgeEarlyEvidence(Instant olderThan) {
            int size = earlyEvidence.size();
            earlyEvidence.clear();
            return size;
        }
    }
}
