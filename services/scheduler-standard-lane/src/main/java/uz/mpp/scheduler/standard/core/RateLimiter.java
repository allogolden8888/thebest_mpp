package uz.mpp.scheduler.standard.core;

/**
 * apply_token_bucket (service_internal_methods.md §2.2), abstracted over
 * "where the bucket state actually lives" — {@link TokenBucket} (in-JVM,
 * per-instance) and {@link RedisTokenBucket} (shared across every
 * partition/replica via Redis) both implement this.
 *
 * <p>Single atomic {@link #acquireUpTo} rather than a separate
 * {@code available()}-then-{@code tryAcquire()} pair — CODE_REVIEW.md HIGH
 * #5 finding: a check-then-act split is race-free only when the bucket is
 * genuinely single-threaded (the old in-JVM-per-partition design). Once the
 * same bucket state is shared across concurrent callers (multiple Kafka
 * Streams partitions/replicas hitting the same Redis key), a separate
 * check and commit reopens exactly the TOCTOU class of bug this whole
 * session has been fixing elsewhere — so the interface only exposes the
 * atomic form.
 */
public interface RateLimiter {

    /**
     * Atomically grants up to {@code requested} permits; returns how many
     * were actually available (0..requested). Never blocks/negative.
     */
    int acquireUpTo(int requested, long nowEpochMs);
}
