package uz.mpp.operatorsmpp.core;

import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BooleanSupplier;
import java.util.logging.Logger;

/**
 * Самокалибрующийся размер пула потоков — вместо статического
 * {@code MAX_CONCURRENT_SUBMITS}, найденного вручную двоичным поиском на
 * конкретной машине (LATENCY_INVESTIGATION_1500TPS.md, b713c53: 300→100
 * после того, как 300 воркер-потоков вытеснили Netty I/O event loop и
 * уронили саму SMPP-сессию — цена ошибки здесь не "просто медленно", а
 * полная потеря сессии), пул сам находит безопасный размер на каждом
 * деплое.
 *
 * <p>Вариант для operator-smpp-session-manager, в отличие от
 * billing-service/delivery-service: тот же {@code maxConcurrentSubmits}
 * одновременно задаёт размер и {@code ExecutorService pacerWorkerPool}, и
 * {@code Semaphore concurrentSubmitPermits} — оба резервируются в
 * лок-степе одним вызовом {@link #resize}. {@link Semaphore} не даёт
 * прямого {@code setPermits}, поэтому используется {@link ResizableSemaphore}
 * (расширяет защищённый {@code reducePermits}).
 *
 * <p>Сигнал калибратора здесь — не только latency диспетчеризации, но и
 * {@code sessionAlive} (обычно {@code client::isActive}): падение сессии во
 * время шага вверх — самый жёсткий сигнал "откатиться и остановиться",
 * важнее роста latency, поскольку прошлое расследование нашло именно этот
 * failure mode, не просто деградацию throughput.
 */
public final class AdaptiveThreadPoolCalibrator {
    private static final Logger LOG = Logger.getLogger(AdaptiveThreadPoolCalibrator.class.getName());

    public static final int FLOOR = 32;
    private static final double GROWTH_FACTOR = 1.5;
    private static final double DEGRADATION_THRESHOLD = 1.4;

    private final ThreadPoolExecutor pool;
    private final ResizableSemaphore semaphore;
    private final BooleanSupplier sessionAlive;
    private final int ceiling;
    private final long tickIntervalMs;
    private final long windowDurationMs;
    private final LatencyWindow latencyWindow = new LatencyWindow(200);

    private volatile boolean locked = false;
    private int currentSize;
    private int lastGoodSize;
    // Единственный источник истины о том, сколько потоков/permits калибратор
    // СЧИТАЕТ применёнными — НИКОГДА не читается обратно ни из
    // pool.getMaximumPoolSize(), ни из semaphore.availablePermits() (второе
    // в принципе не годится: availablePermits() падает при каждом реальном
    // tryAcquire()/растёт при release() воркеров в проде — это не "общая
    // ёмкость", а "сколько сейчас свободно прямо в эту миллисекунду").
    // Предусловие конструктора: pool и semaphore ОБА уже сконструированы
    // вызывающим кодом (Main.java) ровно на {@code Math.min(FLOOR, ceiling)}
    // — калибратор доверяет этому и просто фиксирует то же число как
    // стартовую точку своей бухгалтерии, не перечитывая их состояние.
    private int appliedSize;
    private double bestP95 = Double.MAX_VALUE;
    private final long calibrationStartMs;
    private long lastTickMs;

    public AdaptiveThreadPoolCalibrator(ThreadPoolExecutor pool, ResizableSemaphore semaphore, BooleanSupplier sessionAlive,
                                         int ceiling, long windowDurationMs, long tickIntervalMs) {
        this.pool = pool;
        this.semaphore = semaphore;
        this.sessionAlive = sessionAlive;
        this.ceiling = ceiling;
        this.windowDurationMs = windowDurationMs;
        this.tickIntervalMs = tickIntervalMs;
        this.currentSize = Math.min(FLOOR, ceiling);
        this.lastGoodSize = this.currentSize;
        this.appliedSize = this.currentSize;
        this.calibrationStartMs = System.currentTimeMillis();
        this.lastTickMs = calibrationStartMs;
    }

    /** Вызывать из воркер-потока после КАЖДОГО успешного submit — latencyMs его диспетчеризации. */
    public void recordTaskLatency(long latencyMs) {
        if (locked) {
            return;
        }
        if (!sessionAlive.getAsBoolean()) {
            forceBackoffAndLock("SMPP session inactive during calibration");
            return;
        }
        latencyWindow.record(latencyMs);
        maybeTick();
    }

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

        if (!sessionAlive.getAsBoolean()) {
            resize(lastGoodSize);
            locked = true;
            LOG.warning("thread pool calibration aborted (SMPP session inactive at tick): locked at " + lastGoodSize + " threads");
            return;
        }

        if (now - calibrationStartMs >= windowDurationMs) {
            lock();
            return;
        }

        Double p95 = latencyWindow.p95();
        if (p95 == null) {
            return;
        }

        if (p95 <= bestP95 * DEGRADATION_THRESHOLD) {
            if (p95 < bestP95) {
                bestP95 = p95;
            }
            lastGoodSize = currentSize;
            int next = Math.min(ceiling, (int) Math.ceil(currentSize * GROWTH_FACTOR));
            if (next == currentSize) {
                lock();
                return;
            }
            currentSize = next;
            resize(next);
            latencyWindow.clear();
        } else {
            resize(lastGoodSize);
            lock();
        }
    }

    /**
     * Абсолютный ресайз пула+семафора на конкретный итоговый размер. Дельта
     * всегда считается от {@link #appliedSize} (собственная бухгалтерия
     * калибратора), НИКОГДА от чтения {@code pool.getMaximumPoolSize()} —
     * см. javadoc поля appliedSize про реальный баг, который эта разница
     * маскировала.
     */
    private void resize(int size) {
        int delta = size - appliedSize;
        if (delta == 0) {
            return;
        }
        if (delta > 0) {
            pool.setMaximumPoolSize(size);
            pool.setCorePoolSize(size);
            semaphore.grow(delta);
        } else {
            pool.setCorePoolSize(size);
            pool.setMaximumPoolSize(size);
            semaphore.shrink(-delta);
        }
        appliedSize = size;
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
     * {@link Semaphore} не даёт публичного способа поменять общее число
     * permits — {@code reducePermits} защищённый специально для этого
     * случая (см. javadoc Semaphore: "useful in subclasses that track
     * resources that become unavailable"). {@code grow}/{@code shrink} —
     * оба неблокирующие: grow добавляет permits немедленно, shrink может
     * временно увести {@code availablePermits()} в отрицательные значения,
     * пока не закроются уже выданные permits — ожидаемо и безопасно
     * (javadoc Semaphore это явно допускает).
     */
    public static final class ResizableSemaphore extends Semaphore {
        public ResizableSemaphore(int permits) {
            super(permits);
        }

        void grow(int delta) {
            if (delta > 0) {
                release(delta);
            }
        }

        void shrink(int delta) {
            if (delta > 0) {
                reducePermits(delta);
            }
        }
    }

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
                return null;
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
