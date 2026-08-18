package uz.mpp.partnersmpp.vault;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Production token source — {@code POST /v1/auth/kubernetes/login}
 * {@code {"role": role, "jwt": <projected ServiceAccount JWT>}}, кеширует
 * client token до истечения TTL с запасом ({@link #RENEW_MARGIN}). Прямое
 * зеркало {@code credential-issuer-service/internal/vault/client.go
 * KubernetesAuthTokenSource} — та же margin (30с), та же кеш-стратегия.
 */
public final class KubernetesAuthTokenSource implements TokenSource {

    /**
     * Обновляем токен за это время до истечения его TTL, не впритык —
     * узкое окно, где токен формально ещё жив, но истечёт до того, как
     * запрос, использующий его, дойдёт до Vault. Совпадает с Go-клиентом.
     */
    static final Duration RENEW_MARGIN = Duration.ofSeconds(30);

    private final String addr;
    private final String role;
    private final Path jwtPath;
    private final HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();

    private final Object lock = new Object();
    private String cachedToken;
    private Instant expiresAt = Instant.EPOCH;

    public KubernetesAuthTokenSource(String addr, String role, Path jwtPath, HttpClient httpClient) {
        this.addr = addr;
        this.role = role;
        this.jwtPath = jwtPath;
        this.httpClient = httpClient;
    }

    @Override
    public String token() throws IOException, InterruptedException {
        synchronized (lock) {
            if (cachedToken != null && Instant.now().isBefore(expiresAt.minus(RENEW_MARGIN))) {
                return cachedToken;
            }

            String jwt;
            try {
                jwt = Files.readString(jwtPath).strip();
            } catch (IOException e) {
                throw new IOException("vault: чтение ServiceAccount JWT (" + jwtPath + "): " + e.getMessage(), e);
            }

            String body = mapper.writeValueAsString(Map.of("role", role, "jwt", jwt));
            HttpRequest request = HttpRequest.newBuilder(URI.create(addr + "/v1/auth/kubernetes/login"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();

            HttpResponse<String> response;
            try {
                response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (IOException e) {
                throw new IOException("vault: login-запрос к " + addr + ": " + e.getMessage(), e);
            }

            if (response.statusCode() != 200) {
                throw new IOException("vault: login отклонён, статус " + response.statusCode() + ": " + response.body());
            }

            JsonNode json = mapper.readTree(response.body());
            String clientToken = json.path("auth").path("client_token").asText(null);
            int leaseDurationSeconds = json.path("auth").path("lease_duration").asInt(0);
            if (clientToken == null || clientToken.isEmpty()) {
                throw new IOException("vault: login-ответ без auth.client_token: " + response.body());
            }

            cachedToken = clientToken;
            expiresAt = Instant.now().plusSeconds(leaseDurationSeconds);
            return cachedToken;
        }
    }
}
