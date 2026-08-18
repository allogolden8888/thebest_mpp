package uz.mpp.partnersmpp.vault;

/**
 * {@code VAULT_TOKEN} напрямую — local/dev/break-glass escape hatch, тот
 * же, что {@code vault} CLI само поддерживает через переменную окружения
 * (не изобретаем новую конвенцию — см. {@code
 * credential-issuer-service/internal/vault/client.go StaticTokenSource}).
 */
public final class StaticTokenSource implements TokenSource {

    private final String staticToken;

    public StaticTokenSource(String staticToken) {
        this.staticToken = staticToken;
    }

    @Override
    public String token() {
        return staticToken;
    }
}
