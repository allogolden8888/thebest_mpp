package uz.mpp.partnersmpp.vault;

import java.io.IOException;

/**
 * Способ получить действующий Vault client token — байт-в-байт то же
 * разделение, что {@code credential-issuer-service/internal/vault/client.go}
 * ({@code TokenSource} interface, две реализации).
 */
public interface TokenSource {
    String token() throws IOException, InterruptedException;
}
