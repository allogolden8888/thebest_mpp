package uz.mpp.partnersmpp.server;

/**
 * handle_bind (service_internal_methods.md §1.2): проверка credentials
 * против local config snapshot. Две реализации: {@link StaticAuthenticator}
 * (один захардкоженный system_id, {@code AUTH_VERIFIER_MODE=env} —
 * bootstrap/break-glass fallback) и {@link VaultAuthenticator} (дефолт —
 * реальный партнёрский конфиг из Configuration Redis через {@link
 * uz.mpp.partnersmpp.config.PartnerConfigStore}, живой {@code
 * config.changes} consumer подменяет карту без рестарта, см. README
 * "Vault-аутентификация").
 */
public interface PartnerAuthenticator {

    record AuthResult(boolean allowed, String partnerId, String applicationId) {
        public static AuthResult reject() {
            return new AuthResult(false, null, null);
        }
    }

    AuthResult authenticate(String systemId, String password);
}