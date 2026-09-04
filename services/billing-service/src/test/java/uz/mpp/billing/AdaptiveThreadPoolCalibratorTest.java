package uz.mpp.billing;

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

    @Test
    void полПулаНеОпускаетсяНижеЗаданногоДажеПриДеградацииЛатентности() {
        // Регрессия на реальную находку (замер 300 TPS, 2026-09-04): критерий
        // калибровки — p95 ОДНОЙ задачи, а узкое место ниже по потоку общее,
        // поэтому рост конкурентности всегда удлиняет отдельную задачу и
        // калибратор откатывался в FLOOR=32. При floor=128 откат обязан
        // остановиться на 128, а не уйти в 32.
        ThreadPoolExecutor pool = newPool();
        AdaptiveThreadPoolCalibrator calibrator =
            new AdaptiveThreadPoolCalibrator(pool, 512, 10_000, 0, 128);

        assertEquals(128, calibrator.currentSize(), "старт должен быть с заданного пола, не с FLOOR");

        // Первый тик задаёт bestP95, следующий — резкая деградация: откат+lock.
        for (int i = 0; i < 25; i++) {
            calibrator.recordTaskLatency(10);
        }
        for (int i = 0; i < 25; i++) {
            calibrator.recordTaskLatency(5000);
        }

        assertTrue(calibrator.isLocked(), "резкая деградация должна зафиксировать калибратор");
        assertTrue(calibrator.lastGoodSize() >= 128,
            "откат не должен опускать пул ниже пола, получено: " + calibrator.lastGoodSize());
        assertTrue(pool.getCorePoolSize() >= 128, "реальный пул тоже не должен уйти ниже пола");
    }

    @Test
    void полОграниченПотолком() {
        ThreadPoolExecutor pool = newPool();
        AdaptiveThreadPoolCalibrator calibrator =
            new AdaptiveThreadPoolCalibrator(pool, 64, 10_000, 0, 256);
        assertEquals(64, calibrator.currentSize(), "пол выше потолка должен быть срезан потолком");
    }
}
