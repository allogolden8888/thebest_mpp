package uz.mpp.partnersmpp.registry;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import uz.mpp.partnersmpp.server.ChannelRegistry;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionHeartbeatSchedulerTest {

    @Test
    void tickHeartbeatsOnlyCurrentSessionsWithTheirEpochs() {
        ChannelRegistry channels = new ChannelRegistry();
        EmbeddedChannel removedChannel = new EmbeddedChannel();
        channels.register("removed", "smpp-old", removedChannel);
        channels.unregisterIfSameChannel("removed", "smpp-old", removedChannel);
        long firstEpoch = channels.register("acme", "smpp-a", new EmbeddedChannel());
        long secondEpoch = channels.register("beta", "smpp-b", new EmbeddedChannel());
        Set<String> heartbeats = ConcurrentHashMap.newKeySet();

        try (SessionHeartbeatScheduler scheduler = new SessionHeartbeatScheduler(
            channels,
            (partnerId, systemId, epoch) -> heartbeats.add(partnerId + ":" + systemId + ":" + epoch),
            Duration.ofSeconds(30)
        )) {
            scheduler.heartbeatCurrentSessions();
        }

        assertEquals(Set.of(
            "acme:smpp-a:" + firstEpoch,
            "beta:smpp-b:" + secondEpoch
        ), heartbeats);
    }

    @Test
    void oneRedisFailureDoesNotPreventOtherSessionsOrLaterTicks() {
        ChannelRegistry channels = new ChannelRegistry();
        channels.register("broken", "smpp-a", new EmbeddedChannel());
        channels.register("healthy", "smpp-b", new EmbeddedChannel());
        AtomicInteger healthyCalls = new AtomicInteger();

        try (SessionHeartbeatScheduler scheduler = new SessionHeartbeatScheduler(
            channels,
            (partnerId, systemId, epoch) -> {
                if (partnerId.equals("broken")) {
                    throw new IllegalStateException("Redis unavailable");
                }
                healthyCalls.incrementAndGet();
                return true;
            },
            Duration.ofSeconds(30)
        )) {
            scheduler.heartbeatCurrentSessions();
            scheduler.heartbeatCurrentSessions();
        }

        assertEquals(2, healthyCalls.get());
    }

    @Test
    void periodicWorkerStopsBeforeCloseReturns() throws InterruptedException {
        ChannelRegistry channels = new ChannelRegistry();
        channels.register("acme", "smpp-a", new EmbeddedChannel());
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch calledTwice = new CountDownLatch(2);
        SessionHeartbeatScheduler scheduler = new SessionHeartbeatScheduler(
            channels,
            (partnerId, systemId, epoch) -> {
                calls.incrementAndGet();
                calledTwice.countDown();
                return true;
            },
            Duration.ofMillis(10)
        );

        scheduler.start();
        assertTrue(calledTwice.await(2, TimeUnit.SECONDS), "periodic heartbeat did not run twice");
        scheduler.close();
        int callsAfterClose = calls.get();
        Thread.sleep(50);

        assertEquals(callsAfterClose, calls.get(), "heartbeat must not run after close returns");
        scheduler.close(); // idempotent shutdown
    }

    @Test
    void rejectsInvalidIntervalAndStartAfterClose() {
        ChannelRegistry channels = new ChannelRegistry();
        assertThrows(IllegalArgumentException.class, () -> new SessionHeartbeatScheduler(
            channels, (partnerId, systemId, epoch) -> true, Duration.ZERO
        ));

        SessionHeartbeatScheduler scheduler = new SessionHeartbeatScheduler(
            channels, (partnerId, systemId, epoch) -> true, Duration.ofSeconds(1)
        );
        scheduler.close();
        assertThrows(IllegalStateException.class, scheduler::start);
    }
}
