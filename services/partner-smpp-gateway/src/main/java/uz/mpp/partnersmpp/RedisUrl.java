package uz.mpp.partnersmpp;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/** Builds the Runtime Redis URI from the discrete ExternalSecret env contract. */
public final class RedisUrl {

    private RedisUrl() {
    }

    public static String buildRuntimeUrl() {
        return buildRuntimeUrl(System::getenv);
    }

    static String buildRuntimeUrl(Function<String, String> env) {
        String override = env.apply("REDIS_RUNTIME_URL");
        if (override != null && !override.isBlank()) {
            return override;
        }
        String host = orDefault(env.apply("REDIS_RUNTIME_HOST"), "redis-runtime.mpp.svc");
        String port = orDefault(env.apply("REDIS_RUNTIME_PORT"), "6379");
        String password = env.apply("REDIS_RUNTIME_PASSWORD");
        if (password == null || password.isEmpty()) {
            return "redis://" + host + ":" + port;
        }
        // Password lives in URI user-info; encode delimiters so a generated
        // secret containing @/:/% cannot change the parsed host or path.
        String encodedPassword = URLEncoder.encode(password, StandardCharsets.UTF_8).replace("+", "%20");
        return "redis://:" + encodedPassword + "@" + host + ":" + port;
    }

    private static String orDefault(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }
}
