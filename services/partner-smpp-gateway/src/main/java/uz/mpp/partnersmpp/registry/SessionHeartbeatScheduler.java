package uz.mpp.partnersmpp.registry;

import uz.mpp.partnersmpp.server.ChannelRegistry;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Periodically renews Runtime Redis TTLs for the SMPP sessions that are still
 * present in this gateway instance's in-memory {@link ChannelRegistry}.
 *
 * <p>The worker is single-threaded and uses fixed delay: a slow Redis request
 * cannot build an unbounded queue of heartbeat runs. Individual failures are
 * isolated so one unavailable key does not stop renewal of the other sessions
 * or permanently cancel future runs.</p>
 */
public final class SessionHeartbeatScheduler implements AutoCloseable {

    @FunctionalInterface
    interface HeartbeatSink {
        boolean heartbeat(String partnerId, String systemId, long sessionEpoch);
    }

    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);

    private final ChannelRegistry channelRegistry;
    private final HeartbeatSink heartbeatSink;
    private final Duration interval;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile ScheduledFuture<?> future;

    public SessionHeartbeatScheduler(
        ChannelRegistry channelRegistry,
        SessionRedisRegistry sessionRegistry,
        Duration interval
    ) {
        this(channelRegistry, sessionRegistry::heartbeat, interval);
    }

    SessionHeartbeatScheduler(
        ChannelRegistry channelRegistry,
        HeartbeatSink heartbeatSink,
        Duration interval
    ) {
        this.channelRegistry = Objects.requireNonNull(channelRegistry, "channelRegistry");
        this.heartbeatSink = Objects.requireNonNull(heartbeatSink, "heartbeatSink");
        this.interval = Objects.requireNonNull(interval, "interval");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("interval must be positive");
        }
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "smpp-session-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Starts the worker once. Repeated calls are harmless. */
    public void start() {
        if (closed.get()) {
            throw new IllegalStateException("heartbeat scheduler is closed");
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }
        long delayMillis = Math.max(1L, interval.toMillis());
        future = executor.scheduleWithFixedDelay(
            this::heartbeatCurrentSessions,
            delayMillis,
            delayMillis,
            TimeUnit.MILLISECONDS
        );
    }

    void heartbeatCurrentSessions() {
        if (closed.get()) {
            return;
        }
        for (ChannelRegistry.ActiveSessionRef session : channelRegistry.activeSessionsSnapshot()) {
            if (closed.get()) {
                return;
            }
            try {
                heartbeatSink.heartbeat(
                    session.partnerId(),
                    session.systemId(),
                    session.sessionEpoch()
                );
            } catch (RuntimeException error) {
                // ScheduledExecutorService suppresses all future executions when
                // a task escapes with an exception, so failures must stay local.
                System.err.println(
                    "SMPP session heartbeat failed for partner=" + session.partnerId()
                        + ", system_id=" + session.systemId() + ": " + error.getMessage()
                );
            }
        }
    }

    /** Stops future ticks and waits for an in-flight tick before returning. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        ScheduledFuture<?> scheduled = future;
        if (scheduled != null) {
            scheduled.cancel(false);
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                executor.awaitTermination(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException interrupted) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
