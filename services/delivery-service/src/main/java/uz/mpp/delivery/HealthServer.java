package uz.mpp.delivery;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@code /healthz}/{@code /readyz}/{@code /metrics} на порту 9090 — та же
 * платформенная конвенция, что у остальных сервисов этой серии (см.
 * billing-service/src/main/java/uz/mpp/billing/HealthServer.java).
 */
public final class HealthServer {

    public final AtomicBoolean ready = new AtomicBoolean(false);
    private final HttpServer server;

    public HealthServer(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/healthz", exchange -> respond(exchange, 200, "ok"));
        server.createContext("/readyz", exchange -> {
            if (ready.get()) {
                respond(exchange, 200, "ready");
            } else {
                respond(exchange, 503, "clients not constructed");
            }
        });
        server.createContext("/metrics", exchange -> respond(exchange, 200,
            "# HELP delivery_service_up Service liveness placeholder\n# TYPE delivery_service_up gauge\ndelivery_service_up 1\n"));
    }

    public void start() {
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    public void stop() {
        server.stop(1);
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
