package uz.mpp.partnersmpp.vault;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Тонкая обёртка над Vault KV v2 HTTP API — READ-сторона Фазы 1
 * (credential-issuer-service — WRITE-сторона того же контракта). Прямой
 * HTTP ({@code java.net.http.HttpClient}, уже в JDK, без новой
 * зависимости — тот же принцип минимализма, что у Go-клиента: три
 * операции не оправдывают полноценный Vault SDK), не клиентская
 * библиотека.
 *
 * <p>Байт-в-байт то же API, что {@code
 * credential-issuer-service/internal/vault/client.go} — mount {@code
 * "mpp"} ({@code infra/terraform/vault-secrets.tf vault_mount.mpp}),
 * {@code GET /v1/{mount}/data/{path}} -> {@code .data.data.<property>}
 * (двойная вложенность {@code data.data} — KV v2 конверт вокруг
 * пользовательских полей), {@code GET /v1/sys/health} без токена (200/429
 * оба здоровы).
 */
public final class VaultClient implements VaultSecretReader {

    private final String addr;
    private final String mount;
    private final TokenSource tokenSource;
    private final HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();

    public VaultClient(String addr, String mount, TokenSource tokenSource, HttpClient httpClient) {
        this.addr = addr;
        this.mount = mount;
        this.tokenSource = tokenSource;
        this.httpClient = httpClient;
    }

    /** {@code kv_path, property} — куда именно в Vault читать. */
    public record KvRef(String kvPath, String property) {
    }

    /**
     * {@code vault://partners/click_uz/main/api_key} -> ({@code
     * partners/click_uz/main}, {@code api_key}) — байт-в-байт та же
     * логика, что {@code vault.ParseCredentialRef} (Go) и {@code
     * infra/secrets/generate_external_secrets.py::vault_kv_path_and_property}
     * (Python): разбиение по ПОСЛЕДНЕМУ {@code /}. Все три реализации
     * обязаны совпадать — иначе путь, по которому
     * credential-issuer-service пишет, разойдётся с путём, по которому
     * этот сервис читает.
     */
    public static KvRef parseCredentialRef(String credentialRef) {
        String prefix = "vault://";
        if (credentialRef == null || !credentialRef.startsWith(prefix)) {
            throw new IllegalArgumentException("vault: credential_ref \"" + credentialRef + "\" не начинается с \"" + prefix + "\"");
        }
        String path = credentialRef.substring(prefix.length());
        int idx = path.lastIndexOf('/');
        if (idx <= 0 || idx == path.length() - 1) {
            throw new IllegalArgumentException("vault: credential_ref \"" + credentialRef + "\" не имеет формы path/property");
        }
        return new KvRef(path.substring(0, idx), path.substring(idx + 1));
    }

    /** {@code GET /v1/{mount}/data/{kvPath}} -> {@code .data.data.<property>} строкой. */
    @Override
    public String readProperty(String kvPath, String property) throws IOException, InterruptedException {
        String token = tokenSource.token();

        HttpRequest request = HttpRequest.newBuilder(URI.create(dataUrl(kvPath)))
            .header("X-Vault-Token", token)
            .GET()
            .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IOException("vault: read-запрос к " + kvPath + ": " + e.getMessage(), e);
        }

        if (response.statusCode() == 404) {
            throw new IOException("vault: путь " + kvPath + " не найден");
        }
        if (response.statusCode() != 200) {
            throw new IOException("vault: read отклонён, статус " + response.statusCode() + ": " + response.body());
        }

        JsonNode root = mapper.readTree(response.body());
        JsonNode value = root.path("data").path("data").path(property);
        if (!value.isTextual()) {
            throw new IOException("vault: свойство \"" + property + "\" по пути " + kvPath + " отсутствует или не строка");
        }
        return value.asText();
    }

    /**
     * {@code GET /v1/sys/health} — для {@code /readyz}. Не требует
     * токена — одна из немногих Vault-ручек, открытых без
     * аутентификации, специально для health-проб. 200 = unsealed+active,
     * 429 = unsealed+standby (тоже ОК — standby-реплика всё ещё
     * обслуживает read/write через HA-прокси в реальном кластере).
     */
    public void ping() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(addr + "/v1/sys/health")).GET().build();
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IOException("vault: health-запрос к " + addr + ": " + e.getMessage(), e);
        }
        if (response.statusCode() != 200 && response.statusCode() != 429) {
            throw new IOException("vault: health-статус " + response.statusCode());
        }
    }

    private String dataUrl(String kvPath) {
        return addr + "/v1/" + mount + "/data/" + kvPath;
    }
}
