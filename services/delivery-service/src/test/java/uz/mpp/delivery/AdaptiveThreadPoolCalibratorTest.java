package uz.mpp.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.Test;

/**
 * Чистая, без живой Kafka/сети — тот же принцип, что {@code KafkaIo.OffsetTracker}
 * вынесена отдельно от {@link KafkaIo#run}: калибратор оперирует только над
 * {@link ThreadPoolExecutor} и синтетическими latency-сэмплами.
 */
class AdaptiveThreadPoolCalibratorTest {

    private ThreadPoolExecutor newPool() {
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }

    @Test
    void растётПриСтабильнойНизкойЗадержкеДоПотолкаИФиксируется() {
        ThreadPoolExecutor pool = newPool();
        // Короткое окно/тик — тест не должен ждать реальные 120с.
        AdaptiveThreadPoolCalibrator calibrator = new AdaptiveThreadPoolCalibrator(pool, 128, 10_000, 0);

        assertEquals(AdaptiveThreadPoolCalibrator.FLOOR, calibrator.currentSize());

        // Стабильная низкая задержка — калибратор должен расти шаг за шагом,
        // пока не упрётся в потолок (128) и не зафиксируется сам.
        for (int step = 0; step < 10 && !calibrator.isLocked(); step++) {
            for (int i = 0; i < 25; i++) {
                calibrator.recordTaskLatency(10); // стабильные 10мс
            }
        }

        assertTrue(calibrator.isLocked(), "калибратор должен зафиксироваться, упёршись в потолок");
        assertEquals(128, calibrator.lastGoodSize(), "должен вырасти ровно до потолка при отсутствии деградации");
        assertEquals(128, pool.getMaximumPoolSize());
        assertEquals(128, pool.getCorePoolSize());
    }

    @Test
    void откатываетсяИФиксируетсяПриРезкойДеградацииЛатентности() {
        ThreadPoolExecutor pool = newPool();
        AdaptiveThreadPoolCalibrator calibrator = new AdaptiveThreadPoolCalibrator(pool, 512, 10_000, 0);

        // Первый шаг — хорошая задержка, калибратор растёт (FLOOR=32 -> 48).
        for (int i = 0; i < 25; i++) {
            calibrator.recordTaskLatency(10);
        }
        assertTrue(!calibrator.isLocked());
        int grownSize = calibrator.currentSize();
        assertTrue(grownSize > AdaptiveThreadPoolCalibrator.FLOOR, "должен был вырасти после хорошего первого шага");

        // Следующий шаг — резкая деградация (тот же класс скачка, что
        // LATENCY_INVESTIGATION_1500TPS.md нашёл реальным, не гипотетическим:
        // 200->p95=1811ms vs 450->p95=254ms).
        for (int i = 0; i < 25; i++) {
            calibrator.recordTaskLatency(1000);
        }

        assertTrue(calibrator.isLocked(), "деградация должна была вызвать немедленную фиксацию");
        assertEquals(AdaptiveThreadPoolCalibrator.FLOOR, calibrator.lastGoodSize(),
            "должен откатиться на последний известный безопасный размер (FLOOR, т.к. деградация случилась на первом же шаге роста)");
        assertEquals(AdaptiveThreadPoolCalibrator.FLOOR, pool.getMaximumPoolSize());
    }

    @Test
    void экстренныйОткатБлокируетСразуНезависимоОтТика() {
        ThreadPoolExecutor pool = newPool();
        AdaptiveThreadPoolCalibrator calibrator = new AdaptiveThreadPoolCalibrator(pool, 512, 10_000, 0);
        for (int i = 0; i < 25; i++) {
            calibrator.recordTaskLatency(10);
        }
        assertTrue(!calibrator.isLocked());
        int lastGood = calibrator.lastGoodSize();

        calibrator.forceBackoffAndLock("собственная сессия обрушилась под давлением пула");

        assertTrue(calibrator.isLocked());
        assertEquals(lastGood, pool.getMaximumPoolSize());
        assertEquals(lastGood, pool.getCorePoolSize());

        // После фиксации — дальнейшие вызовы recordTaskLatency не должны
        // ничего менять (no-op).
        calibrator.recordTaskLatency(5);
        assertEquals(lastGood, pool.getMaximumPoolSize());
    }

    @Test
    void фиксируетсяПоИстечениюОкнаДажеБезДеградации() {
        ThreadPoolExecutor pool = newPool();
        // windowDurationMs=0 — окно калибровки истекает мгновенно.
        AdaptiveThreadPoolCalibrator calibrator = new AdaptiveThreadPoolCalibrator(pool, 512, 0, 0);

        calibrator.recordTaskLatency(10);

        assertTrue(calibrator.isLocked(), "истечение окна должно фиксировать калибратор даже без единого шага деградации");
    }
}
