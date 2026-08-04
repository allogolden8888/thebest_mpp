package uz.mpp.partnersmpp.server;

import java.util.Map;

/** Простая in-memory реализация для тестов/этого среза — см. PartnerAuthenticator докстринг. */
public final class StaticAuthenticator implements PartnerAuthenticator {

    public record Credential(String password, String partnerId, String applicationId) {
    }

    private final Map<String, Credential> bySystemId;

    public StaticAuthenticator(Map<String, Credential> bySystemId) {
        this.bySystemId = bySystemId;
    }

    @Override
    public AuthResult authenticate(String systemId, String password) {
        Credential cred = bySystemId.get(systemId);
        if (cred == null || !cred.password().equals(password)) {
            return AuthResult.reject();
        }
        return new AuthResult(true, cred.partnerId(), cred.applicationId());
    }
}