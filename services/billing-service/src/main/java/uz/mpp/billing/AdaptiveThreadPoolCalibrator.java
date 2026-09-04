package uz.mpp.billing;

import java.util.Arrays;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Logger;

/**
 * Самокалибрующийся размер пула потоков — вместо статического значения,
 * найденного один раз вручную двоичным поиском на конкретной машине (см.
 * LATENCY_INVESTIGATION_1500TPS.md: 128 vs 450 давало 10-кратную разницу в
 * p95 в зависимости от того, AMD 12-core/15GB это было или Mac 8-core/7.75GB
 * — число, скопированное с одной машины на другую, дважды давало реальную
 * измеренную регрессию, см. b713c53), пул сам находит безопасный размер под
 * реальным трафиком на каждом деплое.
 *
 * <p>Алгоритм: старт с консервативного {@link #FLOOR}, шаги вверх (×1.5),
 * пока p95 не деградирует относительно лучшего увиденного, откат на
 * последний известный безопасный размер и фиксация при деградации.
 * {@code ThreadPoolExecutor} (в отличие от голого {@code ExecutorService})
 * можно резать/растить на лету — рестарт процесса не нужен.
 *
 * <p>После окончания калибровочного окна — не шевелится дальше (find once,
 * lock, done): продолжать подстраиваться под живой трафик после того, как
 * стабильный размер найден, добавило бы колебания в и так стабильную
 * работу, которых в задаче не просили.
 */
public final class AdaptiveThreadPoolCalibrator {
    private static final Logger LOG = Logger.getLogger(AdaptiveThreadPoolCalibrator.class.getName());

    public static final int FLOOR = 32;
    private static final double GROWTH_FACTOR = 1.5;
    // +40% от лучшего увиденного p95 — тот же порядок величины скачка, что
    // двоичный поиск в LATENCY_INVESTIGATION_1500TPS.md нашёл РЕЗКИМ, не
    // плавным (200→p95=1811ms, 450→p95=254ms) — небольшой запас, не точная
    // граница, подбирать заново не пытаемся аналитически.
    private static final double DEGRADATION_THRESHOLD = 1.4;

    private final ThreadPoolExecutor pool;
    private final int ceiling;
    private final long tickIntervalMs;
    private final long windowDurationMs;
    private final LatencyWindow latencyWindow = new LatencyWindow(200);

    private volatile boolean locked = false;
    private int currentSize;
    private int lastGoodSize;
    private double bestP95 = Double.MAX_VALUE;
    private final long calibrationStartMs;
    private long lastTickMs;

    public AdaptiveThreadPoolCalibrator(ThreadPoolExecutor pool, int ceiling, long windowDurationMs, long tickIntervalMs) {
        this(pool, ceiling, windowDurationMs, tickIntervalMs, FLOOR);
    }

    /**
     * Вариант с явным полом пула.
     *
     * <p>Зачем понадобился (замер 300 TPS, 2026-09-04): критерий калибровки —
     * p95 латентности ОДНОЙ задачи. Но узкое место ниже по потоку общее (одна
     * SMPP-сессия к оператору), поэтому рост конкурентности закономерно
     * увеличивает время каждой отдельной задачи, хотя суммарная пропускная
     * способность при этом растёт. На таком профиле критерий систематически
     * тянет пул ВНИЗ: живой сервис зафиксировался на {@link #FLOOR}=32
     * ("thread pool calibrated: 32 threads (p95=82.0ms)"), что по закону
     * Литтла даёт потолок 32/0.082 ≈ 390 сообщений/с. При целевых 300/с это
     * загрузка 77% — очередь и хвост неизбежны, а любой дрейф латентности
     * оператора роняет ёмкость ниже входящего потока. Отсюда же и
     * невоспроизводимость: два прогона одной конфигурации дали p99 303мс и
     * 1192мс.
     *
     * <p>Пол не отключает калибровку — она по-прежнему может расти вверх и
     * фиксироваться на деградации; он лишь не даёт ей опуститься ниже
     * значения, заведомо достаточного для целевого потока. Считать по
     * Литтлу: floor >= target_tps * task_latency, с запасом на всплески.
     */
    public AdaptiveThreadPoolCalibrator(ThreadPoolExecutor pool, int ceiling, long windowDurationMs, long tickIntervalMs, int floor) {
        this.pool = pool;
        this.ceiling = ceiling;
        this.windowDurationMs = windowDurationMs;
        this.tickIntervalMs = tickIntervalMs;
        this.currentSize = Math.min(Math.max(1, floor), ceiling);
        this.lastGoodSize = this.currentSize;
        resize(this.currentSize);
        this.calibrationStartMs = System.currentTimeMillis();
        this.lastTickMs = calibrationStartMs;
    }

    /** Вызывать из воркер-потока после КАЖДОГО успешного завершения задачи — latencyMs её обработки. */
    public void recordTaskLatency(long latencyMs) {
        if (locked) {
            return; // после фиксации — no-op, единственная цена в стабильной работе: один volatile-read
        }
        latencyWindow.record(latencyMs);
        maybeTick();
    }

    /**
     * Экстренный сигнал (например, обрыв реального соединения/сессии под
     * давлением пула — не просто рост latency) — немедленный откат на
     * последний известный безопасный размер и фиксация, не дожидаясь
     * следующего планового тика.
     */
    public synchronized void forceBackoffAndLock(String reason) {
        if (locked) {
            return;
        }
        resize(lastGoodSize);
        locked = true;
        LOG.warning("thread pool calibration aborted early (" + reason + "): locked at " + lastGoodSize + " threads");
    }

    private synchronized void maybeTick() {
        if (locked) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastTickMs < tickIntervalMs) {
            return;
        }
        lastTickMs = now;

        if (now - calibrationStartMs >= windowDurationMs) {
            lock();
            return;
        }

        Double p95 = latencyWindow.p95();
        if (p95 == null) {
            return; // недостаточно сэмплов в этом тике для значимого решения — ждём следующего
        }

        if (p95 <= bestP95 * DEGRADATION_THRESHOLD) {
            if (p95 < bestP95) {
                bestP95 = p95;
            }
            lastGoodSize = currentSize;
            int next = Math.min(ceiling, (int) Math.ceil(currentSize * GROWTH_FACTOR));
            if (next == currentSize) {
                lock(); // уже на потолке
                return;
            }
            currentSize = next;
            resize(currentSize);
            latencyWindow.clear();
        } else {
            resize(lastGoodSize);
            lock();
        }
    }

    private void resize(int size) {
        // setMaximumPoolSize нельзя опустить ниже текущего corePoolSize (и
        // наоборот) — порядок вызовов зависит от направления изменения.
        if (size >= pool.getCorePoolSize()) {
            pool.setMaximumPoolSize(size);
            pool.setCorePoolSize(size);
        } else {
            pool.setCorePoolSize(size);
            pool.setMaximumPoolSize(size);
        }
    }

    private void lock() {
        locked = true;
        LOG.info("thread pool calibrated: " + lastGoodSize + " threads (p95=" + bestP95 + "ms)");
    }

    public boolean isLocked() {
        return locked;
    }

    public int currentSize() {
        return currentSize;
    }

    public int lastGoodSize() {
        return lastGoodSize;
    }

    /**
     * Скользящее окно последних N latency-сэмплов. Полностью synchronized —
     * критическая секция маленькая (запись в массив + счётчик), активна
     * ТОЛЬКО во время калибровочного окна (после {@link #lock()} —
     * {@link #recordTaskLatency} возвращается до захвата лока вообще), не в
     * установившемся режиме навсегда — тот же класс структуры, что уже
     * потребовал шардирования в policy-service под 1024-way конкуренцией
     * НАВСЕГДА (см. WEAK_HARDWARE_AUDIT.md находка 1.4/раздел про
     * ShardedRuntimeState), но здесь окно ограничено по времени и
     * контенция не накапливается бесконечно — если профилирование под
     * реальной нагрузкой покажет иное, это первое место для шардирования.
     */
    static final class LatencyWindow {
        private final long[] samples;
        private int index = 0;
        private int count = 0;

        LatencyWindow(int capacity) {
            this.samples = new long[capacity];
        }

        synchronized void record(long value) {
            samples[index] = value;
            index = (index + 1) % samples.length;
            if (count < samples.length) {
                count++;
            }
        }

        synchronized Double p95() {
            if (count < 20) {
                return null; // недостаточно сэмплов для значимого перцентиля
            }
            long[] copy = Arrays.copyOf(samples, count);
            Arrays.sort(copy);
            int idx = (int) Math.ceil(0.95 * count) - 1;
            return (double) copy[Math.max(0, Math.min(idx, count - 1))];
        }

        synchronized void clear() {
            count = 0;
            index = 0;
        }
    }
}
