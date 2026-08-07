package uz.mpp.scheduler.standard.core;

import java.util.function.Function;

/**
 * Runtime Redis connection string — k8s injects {@code REDIS_RUNTIME_HOST}/
 * {@code REDIS_RUNTIME_PORT}/{@code REDIS_RUNTIME_PASSWORD} discretely
 * ({@code envFrom: secretRef}, same convention already used across this
 * session — see {@code uz.mpp.billing.RedisUrl} in billing-service, and the
 * Go services' {@code REDIS_RUNTIME_*} env vars, e.g.
 * operator-http-gateway/scheduler-critical-sweep). {@code redis-runtime} is
 * already listed as a secret dependency for scheduler-standard-lane in
 * {@code k8s/generate_manifests.py} — provisioned but unused until now
 * (see CODE_REVIEW.md HIGH #5 / RedisTokenBucket).
 *
 * <p>Accepts a lookup function parameter (not {@code System.getenv()}
 * directly) so every branch (with/without password, explicit override) is
 * testable without reflection hacks — same reason billing-service's
 * version does this.
 */
public final class RedisUrl {

    private RedisUrl() {
    }

    public static String buildRuntimeUrl() {
        return buildRuntimeUrl(System::getenv);
    }

    static String buildRuntimeUrl(Function<String, String> env) {
        String override = env.apply("REDIS_RUNTIME_URL");
        if (override != null && !override.isEmpty()) {
            return override;
        }
        String host = orDefault(env.apply("REDIS_RUNTIME_HOST"), "redis-runtime.mpp.svc");
        String port = orDefault(env.apply("REDIS_RUNTIME_PORT"), "6379");
        String password = env.apply("REDIS_RUNTIME_PASSWORD");
        if (password == null || password.isEmpty()) {
            return "redis://" + host + ":" + port + "/0";
        }
        return "redis://:" + password + "@" + host + ":" + port + "/0";
    }

    private static String orDefault(String value, String fallback) {
        return (value == null || value.isEmpty()) ? fallback : value;
    }
}
