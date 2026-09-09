package uz.mpp.delivery;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Backpressure непрерывного цикла опроса ({@link KafkaIo#shouldPause}).
 *
 * <p>Прежний цикл ограничивал себя сам: следующий {@code poll()} не
 * начинался, пока не слит предыдущий батч — ценой head-of-line blocking,
 * ради устранения которого барьер и убран. Теперь ограничение явное:
 * счётчик in-flight + {@code pause()}/{@code resume()}. Логика решения
 * вынесена чистой функцией и проверяется здесь без Kafka; семантику
 * коммита проверяет {@link OffsetWatermarkTrackerTest}.
 */
class KafkaIoTest {

    @Test
    void doesNotPauseBelowCeiling() {
        assertFalse(KafkaIo.shouldPause(0, 100, false));
        assertFalse(KafkaIo.shouldPause(99, 100, false));
    }

    @Test
    void pausesAtCeiling() {
        assertTrue(KafkaIo.shouldPause(100, 100, false));
        assertTrue(KafkaIo.shouldPause(140, 100, false));
    }

    @Test
    void staysPausedUntilHalfDrained() {
        // Гистерезис: без него счётчик колеблется вокруг потолка и цикл
        // дёргает pause/resume на каждой итерации, сбрасывая накопленный фетч.
        assertTrue(KafkaIo.shouldPause(99, 100, true));
        assertTrue(KafkaIo.shouldPause(51, 100, true));
        assertFalse(KafkaIo.shouldPause(50, 100, true));
        assertFalse(KafkaIo.shouldPause(0, 100, true));
    }

    @Test
    void ceilingOfOneStillMakesProgress() {
        // Патологическая настройка DELIVERY_MAX_IN_FLIGHT=1 не должна
        // залипать: с нулём in-flight опрос обязан возобновиться.
        assertTrue(KafkaIo.shouldPause(1, 1, false));
        assertFalse(KafkaIo.shouldPause(0, 1, true));
    }
}
