package uz.mpp.partnersmpp.server;

/**
 * handle_bind (service_internal_methods.md §1.2): проверка credentials
 * против local config snapshot. В этом срезе — интерфейс с простой
 * in-memory реализацией ({@link StaticAuthenticator}); реальный источник
 * (partner config snapshot из config.changes) не подключён, см. README
 * "Что НЕ реализовано".
 */
public interface PartnerAuthenticator {

    record AuthResult(boolean allowed, String partnerId, String applicationId) {
        public static AuthResult reject() {
            return new AuthResult(false, null, null);
        }
    }

    AuthResult authenticate(String systemId, String password);
}