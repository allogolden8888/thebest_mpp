package uz.mpp.deliveryreconciliation.health;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

/** /healthz, /readyz, /metrics на :9090 — та же конвенция, что и остальные сервисы. */
public final class HealthServer {

    private final AtomicBoolean ready = new AtomicBoolean(false);
    private HttpServer server;

    public void setReady(boolean value) {
        ready.set(value);
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
            boolean r = ready.get();
            byte[] body = (r ? "ready" : "not ready").getBytes();
            exchange.sendResponseHeaders(r ? 200 : 503, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        server.createContext("/metrics", exchange -> {
            byte[] body = ("# HELP delivery_reconciliation_service_up Service liveness placeholder\n"
                + "# TYPE delivery_reconciliation_service_up gauge\ndelivery_reconciliation_service_up 1\n").getBytes();
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