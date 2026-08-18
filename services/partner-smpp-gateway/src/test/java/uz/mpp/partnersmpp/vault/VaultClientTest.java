package uz.mpp.partnersmpp.vault;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Реальный round-trip против {@code vault server -dev} на {@code
 * http://127.0.0.1:8200} (root token {@code root}, mount {@code mpp} KV v2
 * — тот же локальный dev-сервер, против которого
 * credential-issuer-service уже тестируется, см. его README) — не мок
 * HTTP-транспорта. Тест сам сеет данные прямым HTTP POST на {@code
 * /v1/mpp/data/...} (тот же протокол, который {@link VaultClient} читает
 * обратно) — {@link VaultClient} не реализует запись (это WRITE-сторона,
 * которой владеет credential-issuer-service), поэтому тест не может
 * использовать сам клиент для сидирования.
 */
class VaultClientTest {

    private static final String VAULT_ADDR = "http://127.0.0.1:8200";
    private static final String ROOT_TOKEN = "root";
    private static final HttpClient RAW_CLIENT = HttpClient.newHttpClient();

    private static VaultClient client(TokenSource tokenSource) {
        return new VaultClient(VAULT_ADDR, "mpp", tokenSource, HttpClient.newHttpClient());
    }

    private static void seed(String kvPath, String property, String value) throws Exception {
        String body = "{\"data\": {\"" + property + "\": \"" + value + "\"}}";
        HttpRequest request = HttpRequest.newBuilder(URI.create(VAULT_ADDR + "/v1/mpp/data/" + kvPath))
            .header("X-Vault-Token", ROOT_TOKEN)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .build();
        HttpResponse<String> response = RAW_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertTrue(response.statusCode() == 200 || response.statusCode() == 204,
            "seed не удался: " + response.statusCode() + " " + response.body());
    }

    @Test
    void readPropertyReturnsValueWrittenToRealVault() throws Exception {
        String kvPath = "partners/test_" + UUID.randomUUID() + "/smpp";
        seed(kvPath, "bind_password", "s3cr3t-value");

        VaultClient client = client(new StaticTokenSource(ROOT_TOKEN));
        String value = client.readProperty(kvPath, "bind_password");

        assertEquals("s3cr3t-value", value);
    }

    @Test
    void readPropertyThrowsOnMissingPath() {
        VaultClient client = client(new StaticTokenSource(ROOT_TOKEN));
        assertThrows(Exception.class, () -> client.readProperty("partners/does_not_exist_" + UUID.randomUUID(), "x"));
    }

    @Test
    void readPropertyThrowsOnMissingProperty() throws Exception {
        String kvPath = "partners/test_" + UUID.randomUUID() + "/smpp";
        seed(kvPath, "other_property", "value");

        VaultClient client = client(new StaticTokenSource(ROOT_TOKEN));
        assertThrows(Exception.class, () -> client.readProperty(kvPath, "bind_password"));
    }

    @Test
    void pingSucceedsAgainstRealDevServer() throws Exception {
        VaultClient client = client(new StaticTokenSource(ROOT_TOKEN));
        assertDoesNotThrow(client::ping);
    }

    @Test
    void readPropertyFailsWithWrongToken() throws Exception {
        String kvPath = "partners/test_" + UUID.randomUUID() + "/smpp";
        seed(kvPath, "bind_password", "s3cr3t-value");

        VaultClient client = client(new StaticTokenSource("definitely-not-a-valid-token"));
        assertThrows(Exception.class, () -> client.readProperty(kvPath, "bind_password"));
    }

    @Test
    void parseCredentialRefSplitsOnLastSlash() {
        VaultClient.KvRef ref = VaultClient.parseCredentialRef("vault://partners/click_uz/main/api_key");
        assertEquals("partners/click_uz/main", ref.kvPath());
        assertEquals("api_key", ref.property());
    }

    @Test
    void parseCredentialRefRejectsMissingPrefix() {
        assertThrows(IllegalArgumentException.class, () -> VaultClient.parseCredentialRef("partners/click_uz/main/api_key"));
    }

    @Test
    void parseCredentialRefRejectsMissingProperty() {
        assertThrows(IllegalArgumentException.class, () -> VaultClient.parseCredentialRef("vault://partners/click_uz/main/"));
    }

    @Test
    void staticTokenSourceReturnsConfiguredToken() throws Exception {
        assertEquals("abc123", new StaticTokenSource("abc123").token());
    }
}
