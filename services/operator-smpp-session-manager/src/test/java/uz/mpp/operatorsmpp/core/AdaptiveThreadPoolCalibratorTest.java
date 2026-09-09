package uz.mpp.operatorsmpp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import uz.mpp.operatorsmpp.core.AdaptiveThreadPoolCalibrator.ResizableSemaphore;

/**
 * Чистая, без живого SMPP/сети. Отдельно от billing-service/delivery-service
 * вариантов: здесь ещё и {@link ResizableSemaphore} растёт/сжимается в
 * лок-степе с пулом, плюс сигнал обрыва сессии.
 */
class AdaptiveThreadPoolCalibratorTest {

    /**
     * Предусловие конструктора калибратора (см. его javadoc): pool и
     * semaphore ОБА стартуют на FLOOR — та же конвенция, что реальный
     * Main.java уже соблюдает.
     */
    private ThreadPoolExecutor newPool() {
        return new ThreadPoolExecutor(
            AdaptiveThreadPoolCalibrator.FLOOR, AdaptiveThreadPoolCalibrator.FLOOR,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }

    @Test
    void растётИСинхронноУвеличиваетСемафор() {
        ThreadPoolExecutor pool = newPool();
        ResizableSemaphore sem = new ResizableSemaphore(AdaptiveThreadPoolCalibrator.FLOOR);
        AdaptiveThreadPoolCalibrator calibrator = new AdaptiveThreadPoolCalibrator(
            pool, sem, () -> true, 128, 10_000, 0);

        for (int step = 0; step < 10 && !calibrator.isLocked(); step++) {
            for (int i = 0; i < 25; i++) {
                calibrator.recordTaskLatency(10);
            }
        }

        assertTrue(calibrator.isLocked());
        assertEquals(128, calibrator.lastGoodSize());
        assertEquals(128, pool.getMaximumPoolSize());
        assertEquals(128, sem.availablePermits(), "семафор должен вырасти синхронно с пулом");
    }

    @Test
    void обрывСессииВоВремяКалибровкиНемедленноОткатывает() {
        ThreadPoolExecutor pool = newPool();
        ResizableSemaphore sem = new ResizableSemaphore(AdaptiveThreadPoolCalibrator.FLOOR);
        boolean[] sessionAlive = {true};
        AdaptiveThreadPoolCalibrator calibrator = new AdaptiveThreadPoolCalibrator(
            pool, sem, () -> sessionAlive[0], 512, 10_000, 0);

        // Один успешный шаг роста.
        for (int i = 0; i < 25; i++) {
            calibrator.recordTaskLatency(10);
        }
        assertTrue(!calibrator.isLocked());
        int lastGood = calibrator.lastGoodSize();

        // Сессия падает — следующий же вызов recordTaskLatency должен
        // немедленно откатить и зафиксировать, ДАЖЕ если latency сама по
        // себе выглядит нормально (прошлое расследование нашло именно этот
        // failure mode: сессия гибнет под давлением пула, не просто медленно
        // отвечает).
        sessionAlive[0] = false;
        calibrator.recordTaskLatency(10);

        assertTrue(calibrator.isLocked(), "обрыв сессии должен немедленно зафиксировать калибратор");
        assertEquals(lastGood, pool.getMaximumPoolSize());
        assertEquals(lastGood, sem.availablePermits());
    }

    @Test
    void сжатиеУменьшаетИПулИСемафорВместе() {
        ThreadPoolExecutor pool = newPool();
        ResizableSemaphore sem = new ResizableSemaphore(AdaptiveThreadPoolCalibrator.FLOOR);
        AdaptiveThreadPoolCalibrator calibrator = new AdaptiveThreadPoolCalibrator(
            pool, sem, () -> true, 512, 10_000, 0);

        // Хороший первый шаг роста (FLOOR=32 -> 48).
        for (int i = 0; i < 25; i++) {
            calibrator.recordTaskLatency(10);
        }
        int grown = calibrator.currentSize();
        assertTrue(grown > AdaptiveThreadPoolCalibrator.FLOOR);

        // Резкая деградация -> откат на FLOOR.
        for (int i = 0; i < 25; i++) {
            calibrator.recordTaskLatency(1000);
        }

        assertTrue(calibrator.isLocked());
        assertEquals(AdaptiveThreadPoolCalibrator.FLOOR, pool.getMaximumPoolSize());
        assertEquals(AdaptiveThreadPoolCalibrator.FLOOR, sem.availablePermits());
    }

    /**
     * Регрессия на реальную находку: {@code maybeTick()} вызывается ТОЛЬКО из
     * {@code recordTaskLatency()}, поэтому калибровочное окно могло целиком
     * пройти на простое — и первый же вызов после начала нагрузки фиксировал
     * пул, не собрав ни одного сэмпла. В логах operator-smpp-session-manager
     * это до сих пор видно как нетронутый Double.MAX_VALUE:
     * "thread pool calibrated: 32 threads (p95=1.7976931348623157E308ms)".
     * Тот же баг уже исправлен в delivery-service и billing-service; здесь он
     * дороже, потому что залипший размер держит ещё и число permits
     * SMPP-сессии.
     */
    @Test
    void неФиксируетсяПоИстечениюОкнаЕслиТрафикаНеБыло() {
        ThreadPoolExecutor pool = newPool();
        ResizableSemaphore sem = new ResizableSemaphore(AdaptiveThreadPoolCalibrator.FLOOR);
        // windowDurationMs=0 — окно калибровки истекает мгновенно.
        AdaptiveThreadPoolCalibrator calibrator = new AdaptiveThreadPoolCalibrator(
            pool, sem, () -> true, 512, 0, 0);

        // Меньше порога значимости LatencyWindow — валидного p95 нет.
        for (int i = 0; i < 5; i++) {
            calibrator.recordTaskLatency(10);
        }

        assertTrue(!calibrator.isLocked(),
            "без единого валидного p95 фиксировать нечего — окно должно отсчитываться заново");
        assertEquals(AdaptiveThreadPoolCalibrator.FLOOR, pool.getMaximumPoolSize(),
            "пул не должен шевелиться, пока окно перезапускается");
        assertEquals(AdaptiveThreadPoolCalibrator.FLOOR, sem.availablePermits(),
            "семафор не должен шевелиться, пока окно перезапускается");

        // Как только данные реально набрались — фиксация происходит штатно.
        for (int i = 0; i < 25; i++) {
            calibrator.recordTaskLatency(10);
        }
        assertTrue(calibrator.isLocked(), "с набранными сэмплами истечение окна обязано зафиксировать пул");
    }
}
