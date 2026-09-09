package uz.mpp.delivery;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

/**
 * Трекер НЕПРЕРЫВНОГО (contiguous) watermark'а завершённых оффсетов —
 * ядро безопасности коммита в новом цикле {@link KafkaIo#run}.
 *
 * <p>Зачем понадобился. Прежний {@code KafkaIo.OffsetTracker} (и его
 * Go-двойник {@code services/dlr-manager/internal/kafkaio/offset_tracker.go},
 * взятый здесь за образец семантики) жил ровно один poll: батч
 * обрабатывался целиком, записи одной партиции сливались строго по
 * возрастанию оффсета, и «последний успешный» был автоматически
 * непрерывным префиксом. Это работало только потому, что цикл ЖДАЛ весь
 * батч перед коммитом — то есть ровно из-за того барьера, который мы и
 * убираем. Как только записи начинают завершаться конкурентно и не по
 * порядку, «последний успешный оффсет» перестаёт быть безопасным: если
 * оффсет 12 завершился, а 10 ещё в работе, коммит 13 УТЕРЯЕТ запись 10
 * при падении процесса. Поэтому здесь хранится не «последний успешный», а
 * граница непрерывного префикса завершённых.
 *
 * <p>Модель. На партицию: {@code watermark} — первый оффсет, который ещё
 * НЕ завершён (он же значение, которое корректно передавать в
 * {@code OffsetAndMetadata}: Kafka трактует коммит как «следующий к
 * чтению»); {@code completedAhead} — множество уже завершённых оффсетов
 * ЗА дырой. Завершение оффсета кладётся в множество, после чего watermark
 * протягивается вперёд, пока следующий оффсет находится в множестве.
 * Размер {@code completedAhead} ограничен потолком in-flight записей
 * (см. {@code DELIVERY_MAX_IN_FLIGHT}), неограниченно расти не может.
 *
 * <p>Эпохи. Ребаланс может отобрать партицию, пока её записи ещё в
 * работе. Завершение «старой» записи, применённое после того, как
 * партиция вернулась к нам и была перечитана с закоммиченного оффсета,
 * пометило бы готовым оффсет, который на самом деле ещё обрабатывается —
 * то есть тихая потеря сообщения. Поэтому каждое завершение несёт эпоху,
 * выданную в момент диспатча, а {@link #reset()} (вызывается из
 * {@code onPartitionsRevoked}) увеличивает эпоху; завершения прошлых
 * эпох отбрасываются, их записи будут переданы заново — штатный
 * at-least-once.
 *
 * <p>НЕ потокобезопасен намеренно: все методы вызываются ТОЛЬКО из потока
 * опроса (KafkaConsumer сам не потокобезопасен, поэтому поток опроса всё
 * равно один). Воркеры кладут завершения в concurrent-очередь, а
 * применяет их в трекер поток опроса — см. {@code applyCompletions}.
 */
final class OffsetWatermarkTracker {

    private final Map<TopicPartition, Progress> byPartition = new HashMap<>();
    private long epoch = 1;

    private static final class Progress {
        /** Первый ЕЩЁ НЕ завершённый оффсет — он же то, что коммитим. */
        long watermark;
        /** Уже закоммиченное значение — чтобы не слать один и тот же оффсет повторно. */
        long committed;
        /** Завершённые оффсеты ЗА дырой; хранятся до тех пор, пока дыра не закроется. */
        final Set<Long> completedAhead = new HashSet<>();

        Progress(long firstOffset) {
            this.watermark = firstOffset;
            // Стартовое значение = позиция, с которой партицию и так читают:
            // коммитить её незачем, прогресса она не отражает.
            this.committed = firstOffset;
        }
    }

    /** Текущая эпоха — её надо запомнить в момент диспатча записи и вернуть вместе с завершением. */
    long epoch() {
        return epoch;
    }

    /**
     * Инвалидирует все незавершённые записи (ребаланс/остановка): завершения
     * прошлых эпох будут отброшены, состояние партиций забыто.
     */
    void reset() {
        byPartition.clear();
        epoch++;
    }

    /**
     * Записи ОБЯЗАНЫ регистрироваться в порядке возрастания оффсета внутри
     * партиции — это гарантирует сам {@code poll()}, и на этом стоит
     * инициализация watermark'а первым увиденным оффсетом.
     */
    void register(TopicPartition partition, long offset) {
        Progress progress = byPartition.get(partition);
        if (progress == null) {
            byPartition.put(partition, new Progress(offset));
        }
        // offset < watermark здесь не бывает: poll не отдаёт назад уже
        // отданное без seek/ребаланса, а ребаланс проходит через reset().
    }

    /**
     * Отмечает оффсет завершённым (оба подтверждения получены — Kafka-ack
     * публикации stage.completed и запись исхода в Redis) ЛИБО признанным
     * безнадёжным (см. poison-pill в {@link KafkaIo}).
     *
     * @return true, если завершение применено; false — если оно от прошлой
     *         эпохи, для незнакомой партиции или для оффсета ниже watermark'а
     *         (дубликат).
     */
    boolean complete(long completionEpoch, TopicPartition partition, long offset) {
        if (completionEpoch != epoch) {
            return false;
        }
        Progress progress = byPartition.get(partition);
        if (progress == null || offset < progress.watermark) {
            return false;
        }
        progress.completedAhead.add(offset);
        while (progress.completedAhead.remove(progress.watermark)) {
            progress.watermark++;
        }
        return true;
    }

    /**
     * Оффсеты, которые есть смысл коммитить прямо сейчас: только партиции, чей
     * watermark продвинулся с прошлого коммита. Пустая карта — коммитить нечего,
     * запрос к брокеру не нужен вовсе.
     */
    Map<TopicPartition, Long> committableOffsets() {
        Map<TopicPartition, Long> result = new HashMap<>();
        byPartition.forEach((partition, progress) -> {
            if (progress.watermark > progress.committed) {
                result.put(partition, progress.watermark);
            }
        });
        return result;
    }

    /**
     * Все текущие watermark'и, независимо от того, коммитились ли они уже —
     * для финального синхронного коммита при остановке: последний
     * {@code commitAsync} мог не долететь, а повторить его дешевле, чем
     * передоставлять сообщения.
     */
    Map<TopicPartition, Long> allWatermarks() {
        Map<TopicPartition, Long> result = new HashMap<>();
        byPartition.forEach((partition, progress) -> result.put(partition, progress.watermark));
        return result;
    }

    /** Оптимистичная отметка «отправлено брокеру» — см. обработку ошибки commitAsync в {@link KafkaIo}. */
    void markCommitted(Map<TopicPartition, Long> offsets) {
        offsets.forEach((partition, offset) -> {
            Progress progress = byPartition.get(partition);
            if (progress != null && offset > progress.committed) {
                progress.committed = offset;
            }
        });
    }

    /** Диагностика/тесты: сколько завершений висит за дырами (косвенный признак застрявшей записи). */
    int gapsAhead(TopicPartition partition) {
        Progress progress = byPartition.get(partition);
        return progress == null ? 0 : progress.completedAhead.size();
    }
}
