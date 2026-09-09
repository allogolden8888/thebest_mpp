package uz.mpp.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * {@link OffsetWatermarkTracker} — чистая логика, без живой Kafka и без
 * единого потока: {@link TopicPartition} здесь только структура-ключ.
 *
 * <p>Проверяем ровно то, ради чего трекер и заменил прежний
 * {@code KafkaIo.OffsetTracker}: при конкурентной обработке записи
 * завершаются не по порядку, и «последний успешный оффсет» коммитить
 * НЕЛЬЗЯ — коммитится только непрерывный префикс завершённых.
 */
class OffsetWatermarkTrackerTest {

    private static final TopicPartition TP = new TopicPartition("stage.delivery", 0);

    private static OffsetWatermarkTracker trackerWith(long... offsets) {
        OffsetWatermarkTracker tracker = new OffsetWatermarkTracker();
        for (long offset : offsets) {
            tracker.register(TP, offset);
        }
        return tracker;
    }

    @Test
    void nothingToCommitBeforeAnythingCompletes() {
        OffsetWatermarkTracker tracker = trackerWith(10, 11, 12);
        assertEquals(Map.of(), tracker.committableOffsets(),
            "регистрация записи — не прогресс: коммитить позицию, с которой её только что прочитали, незачем");
    }

    @Test
    void contiguousCompletionAdvancesWatermark() {
        OffsetWatermarkTracker tracker = trackerWith(10, 11, 12);
        tracker.complete(tracker.epoch(), TP, 10);
        assertEquals(Map.of(TP, 11L), tracker.committableOffsets());
        tracker.complete(tracker.epoch(), TP, 11);
        tracker.complete(tracker.epoch(), TP, 12);
        assertEquals(Map.of(TP, 13L), tracker.committableOffsets());
    }

    @Test
    void holeBlocksCommitPastIt() {
        OffsetWatermarkTracker tracker = trackerWith(10, 11, 12);
        // 12 завершилась первой (быстрый оператор), 10 ещё в работе.
        tracker.complete(tracker.epoch(), TP, 12);
        assertEquals(Map.of(), tracker.committableOffsets(),
            "коммит 13 здесь потерял бы записи 10 и 11 при падении процесса");
        assertEquals(1, tracker.gapsAhead(TP));

        tracker.complete(tracker.epoch(), TP, 11);
        assertEquals(Map.of(), tracker.committableOffsets(), "дыра на 10 всё ещё держит watermark");

        // Закрытие дыры протягивает watermark сразу через весь накопленный хвост.
        tracker.complete(tracker.epoch(), TP, 10);
        assertEquals(Map.of(TP, 13L), tracker.committableOffsets());
        assertEquals(0, tracker.gapsAhead(TP), "закрытые дыры не должны копиться в памяти");
    }

    @Test
    void outOfOrderCompletionInArbitraryOrderEndsAtSameWatermark() {
        OffsetWatermarkTracker tracker = trackerWith(100, 101, 102, 103, 104);
        for (long offset : new long[] {103, 101, 104, 100, 102}) {
            tracker.complete(tracker.epoch(), TP, offset);
        }
        assertEquals(Map.of(TP, 105L), tracker.committableOffsets());
    }

    @Test
    void committedOffsetsAreNotOfferedTwice() {
        OffsetWatermarkTracker tracker = trackerWith(10, 11);
        tracker.complete(tracker.epoch(), TP, 10);
        Map<TopicPartition, Long> first = tracker.committableOffsets();
        assertEquals(Map.of(TP, 11L), first);

        tracker.markCommitted(first);
        assertEquals(Map.of(), tracker.committableOffsets(), "повторный коммит того же оффсета — лишний round-trip");

        tracker.complete(tracker.epoch(), TP, 11);
        assertEquals(Map.of(TP, 12L), tracker.committableOffsets(), "новый прогресс обязан снова стать коммитабельным");
    }

    @Test
    void allWatermarksIgnoreAlreadyCommittedMark() {
        // Финальный коммит при остановке обязан повторить последний оффсет,
        // даже если commitAsync уже был отправлен и мог не долететь.
        OffsetWatermarkTracker tracker = trackerWith(10);
        tracker.complete(tracker.epoch(), TP, 10);
        tracker.markCommitted(tracker.committableOffsets());
        assertEquals(Map.of(TP, 11L), tracker.allWatermarks());
    }

    @Test
    void duplicateAndBelowWatermarkCompletionsAreIgnored() {
        OffsetWatermarkTracker tracker = trackerWith(10, 11);
        assertTrue(tracker.complete(tracker.epoch(), TP, 10));
        assertFalse(tracker.complete(tracker.epoch(), TP, 10), "повтор завершения не должен двигать watermark");
        assertEquals(Map.of(TP, 11L), tracker.committableOffsets());
    }

    @Test
    void partitionsAreIndependent() {
        TopicPartition tp0 = new TopicPartition("stage.delivery", 0);
        TopicPartition tp1 = new TopicPartition("stage.delivery", 1);
        OffsetWatermarkTracker tracker = new OffsetWatermarkTracker();
        tracker.register(tp0, 5);
        tracker.register(tp0, 6);
        tracker.register(tp1, 90);

        tracker.complete(tracker.epoch(), tp0, 5);
        tracker.complete(tracker.epoch(), tp1, 90);
        // Дыра на tp0 (6 ещё в работе) не должна мешать tp1 коммититься.
        assertEquals(Map.of(tp0, 6L, tp1, 91L), tracker.committableOffsets());
    }

    @Test
    void completionForUnknownPartitionIsIgnored() {
        OffsetWatermarkTracker tracker = new OffsetWatermarkTracker();
        assertFalse(tracker.complete(tracker.epoch(), TP, 10));
        assertEquals(Map.of(), tracker.committableOffsets());
    }

    @Test
    void staleEpochCompletionCannotAdvanceReassignedPartition() {
        // Сценарий ребаланса: записи 10..12 в работе, партицию отобрали,
        // потом вернули, и мы перечитали её с 10. Опоздавшее завершение
        // «оффсет 12 готов» от прошлой жизни обязано быть отброшено — иначе
        // коммит проскочил бы мимо реально ещё не обработанных записей.
        OffsetWatermarkTracker tracker = trackerWith(10, 11, 12);
        long staleEpoch = tracker.epoch();

        tracker.reset();
        assertEquals(Map.of(), tracker.allWatermarks(), "reset забывает состояние партиций");

        tracker.register(TP, 10);
        assertFalse(tracker.complete(staleEpoch, TP, 12), "завершение прошлой эпохи должно отбрасываться");
        assertEquals(Map.of(), tracker.committableOffsets());

        assertTrue(tracker.complete(tracker.epoch(), TP, 10), "текущая эпоха работает как обычно");
        assertEquals(Map.of(TP, 11L), tracker.committableOffsets());
    }

    @Test
    void poisonPillCompletionUnblocksTheWholePartition() {
        // Неисправимая запись (см. Dispatcher.fail) помечается завершённой тем
        // же вызовом complete() — иначе один битый оффсет запинил бы коммит
        // партиции навсегда: тот самый класс бага, что уже ловили в
        // pipeline-engine и policy-service.
        OffsetWatermarkTracker tracker = trackerWith(10, 11, 12);
        tracker.complete(tracker.epoch(), TP, 11);
        tracker.complete(tracker.epoch(), TP, 12);
        assertEquals(Map.of(), tracker.committableOffsets());

        tracker.complete(tracker.epoch(), TP, 10); // poison pill: обработать нельзя, но watermark продвигаем
        assertEquals(Map.of(TP, 13L), tracker.committableOffsets());
    }
}
