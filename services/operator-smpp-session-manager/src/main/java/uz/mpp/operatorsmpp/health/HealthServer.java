package uz.mpp.operatorsmpp.health;

import com.sun.net.httpserver.HttpServer;
import uz.mpp.operatorsmpp.client.OperatorSmppClient;
import uz.mpp.operatorsmpp.core.PacerMetrics;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * /healthz, /readyz, /metrics на :9090 — та же конвенция, что и остальные
 * сервисы. dynamic-seeking-russell.md "Observability": /metrics теперь
 * отдаёт реальные, движущиеся числа пейсера ({@link PacerMetrics}) вместо
 * статичной заглушки — конструктор получил параметры {@code PacerMetrics}/
 * {@code OperatorSmppClient} (та же конвенция параметризованного
 * конструктора, что уже у delivery-service/billing-service HealthServer.java).
 */
public final class HealthServer {

    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final PacerMetrics pacerMetrics;
    private final OperatorSmppClient client;
    private HttpServer server;

    public HealthServer(PacerMetrics pacerMetrics, OperatorSmppClient client) {
        this.pacerMetrics = pacerMetrics;
        this.client = client;
    }

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
            String text = "# HELP operator_smpp_session_manager_up Service liveness placeholder\n"
                + "# TYPE operator_smpp_session_manager_up gauge\noperator_smpp_session_manager_up 1\n"
                + pacerMetrics.renderPrometheusText()
                + "# HELP operator_smpp_pending_response_count Реальная глубина SMPP-окна — сколько submit/bind сейчас ждут ответа оператора\n"
                + "# TYPE operator_smpp_pending_response_count gauge\n"
                + "operator_smpp_pending_response_count " + client.pendingResponseCount() + "\n";
            byte[] body = text.getBytes(StandardCharsets.UTF_8);
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
