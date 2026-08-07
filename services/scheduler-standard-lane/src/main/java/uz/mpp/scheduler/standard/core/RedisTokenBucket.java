package uz.mpp.scheduler.standard.core;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * apply_token_bucket (service_internal_methods.md §2.2) — CODE_REVIEW.md
 * HIGH #5 real fix, not the partition-count-division mitigation this class
 * replaces.
 *
 * <p><b>The bug this fixes:</b> {@code HoldCommandProcessor} used to hold
 * one {@link TokenBucket} instance per stage NAME, but Kafka Streams
 * creates one {@code HoldCommandProcessor} per assigned PARTITION of
 * {@code scheduler.standard.commands} (partitioned by {@code message_id},
 * not {@code stage_name} — {@code data_infrastructure_spec.md}) — so the
 * intended "one bucket per stage" limit actually existed once per
 * partition. With 8 partitions on one instance (worst case, single
 * replica), the real aggregate release rate could be up to 8x the
 * documented limit; with multiple replicas (partitions spread across
 * instances) it's worse still, and rebalances make it worse yet again as
 * partition-to-instance assignment shifts.
 *
 * <p><b>The fix:</b> move the bucket state out of the JVM entirely, into
 * one Redis key per stage name (not per partition, not per instance) — the
 * same {@code apply_atomic_charge.lua}-style pattern billing-service
 * already uses for exactly this class of problem (state that must be
 * correct across every concurrent caller, not just within one process).
 * {@code token_bucket_acquire.lua} does refill + acquire-up-to-N as one
 * atomic server-side operation, so every partition of every replica now
 * shares the true, single per-stage limit — no division-by-partition-count
 * hack, no remaining under/over-limit skew from replica count or
 * rebalances.
 */
public final class RedisTokenBucket implements RateLimiter {

    private static final String SCRIPT = loadScript();

    private final RedisClient client;
    private final String key;
    private final double capacity;
    private final double refillPerSecond;

    public RedisTokenBucket(RedisClient client, String stageName, double capacity, double refillPerSecond) {
        this.client = client;
        this.key = "scheduler:standard:token_bucket:" + stageName;
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
    }

    private static String loadScript() {
        try (InputStream in = RedisTokenBucket.class.getClassLoader().getResourceAsStream("token_bucket_acquire.lua")) {
            if (in == null) {
                throw new IllegalStateException("token_bucket_acquire.lua не найден в classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("не удалось прочитать token_bucket_acquire.lua", e);
        }
    }

    @Override
    public int acquireUpTo(int requested, long nowEpochMs) {
        if (requested <= 0) {
            return 0;
        }
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> commands = connection.sync();
            Long granted = commands.eval(
                SCRIPT, ScriptOutputType.INTEGER, new String[] {key},
                String.valueOf(capacity), String.valueOf(refillPerSecond),
                String.valueOf(nowEpochMs), String.valueOf(requested));
            return granted == null ? 0 : granted.intValue();
        }
    }
}
