package uz.mpp.partnersmpp.health;

import com.sun.net.httpserver.HttpServer;
import uz.mpp.partnersmpp.metrics.SmppMetrics;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/** /healthz, /readyz, /metrics на :9090 — та же конвенция, что и остальные сервисы. */
public final class HealthServer {

    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final Map<String, Callable<Void>> dependencyChecks = new ConcurrentHashMap<>();
    private HttpServer server;

    public void setReady(boolean value) {
        ready.set(value);
    }

    /**
     * Регистрирует реальные health-check функции для внешних зависимостей
     * (Vault, партнёрский конфиг) — тот же принцип, что Go-сервисы этой
     * сессии ({@code internal/health.State::SetDependencyChecks}):
     * {@code /readyz} отражает реальное состояние зависимостей, не
     * статический флаг, выставленный один раз при старте. Полностью
     * заменяет предыдущий набор проверок (не аккумулирует между вызовами).
     */
    public void setDependencyChecks(Map<String, Callable<Void>> checks) {
        dependencyChecks.clear();
        dependencyChecks.putAll(checks);
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(9090), 0);

        server.createContext("/healthz", exchange -> {
            byte[] body = "ok".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        server.createContext("/readyz", exchange -> {
            if (!ready.get()) {
                byte[] body = "not ready".getBytes();
                exchange.sendResponseHeaders(503, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
                return;
            }

            Map<String, String> failed = new LinkedHashMap<>();
            for (Map.Entry<String, Callable<Void>> entry : dependencyChecks.entrySet()) {
                try {
                    entry.getValue().call();
                } catch (Exception e) {
                    failed.put(entry.getKey(), String.valueOf(e.getMessage()));
                }
            }

            byte[] body = (failed.isEmpty() ? "ready" : "not ready: " + failed).getBytes();
            exchange.sendResponseHeaders(failed.isEmpty() ? 200 : 503, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        // BACKOFFICE_ROADMAP.md P1 "Observability": реальные счётчик/
        // гистограмма handleSubmitSm (см. SmppMetrics/SmppServerHandler),
        // не только liveness-заглушка.
        server.createContext("/metrics", exchange -> {
            byte[] body = SmppMetrics.scrape().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        server.setExecutor(null);
        server.start();
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }
}
