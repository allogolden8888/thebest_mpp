package uz.mpp.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HealthServerTest {

    private HealthServer server;
    private int port;
    private final HttpClient client = HttpClient.newHttpClient();

    @BeforeEach
    void start() throws IOException {
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
    void readyz503UntilClientsConstructed() throws Exception {
        assertEquals(503, get("/readyz").statusCode());
        server.ready.set(true);
        assertEquals(200, get("/readyz").statusCode());
    }

    @Test
    void metricsReturns200() throws Exception {
        assertEquals(200, get("/metrics").statusCode());
    }
}
