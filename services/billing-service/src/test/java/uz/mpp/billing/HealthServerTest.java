package uz.mpp.billing;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Реальный HTTP-round-trip на свободном порту — не мок роутера. */
class HealthServerTest {

    private HealthServer server;
    private int port;
    private final HttpClient client = HttpClient.newHttpClient();

    @BeforeEach
    void start() throws IOException {
        // port=0 — ОС сама выдаёт свободный эфемерный порт, не псевдослучайный
        // выбор из диапазона (кодревью: риск конфликта под параллельными прогонами CI).
        server = new HealthServer(0);
        server.start();
        port = server.port();
    }

    @AfterEach
    void stop() {
        server.stop();
    }

    private HttpResponse<String> get(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void healthzAlwaysOk() throws Exception {
        assertEquals(200, get("/healthz").statusCode());
    }

    @Test
    void readyz503UntilTariffLoaded() throws Exception {
        assertEquals(503, get("/readyz").statusCode());
        server.ready.set(true);
        assertEquals(200, get("/readyz").statusCode());
    }

    @Test
    void metricsReturns200() throws Exception {
        assertEquals(200, get("/metrics").statusCode());
    }
}
