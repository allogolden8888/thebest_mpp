package uz.mpp.partnersmpp.server;

import uz.mpp.partnersmpp.config.PartnerConfigLoader.SmppBindCredential;
import uz.mpp.partnersmpp.vault.VaultClient;
import uz.mpp.partnersmpp.vault.VaultSecretReader;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Реальная реализация {@link PartnerAuthenticator} — заменяет {@link
 * StaticAuthenticator} как основной путь (см. {@code Main.java}
 * {@code AUTH_VERIFIER_MODE}, старый статический путь оставлен как
 * bootstrap/break-glass fallback, не удалён). Пароль из
 * {@code bind_transceiver} сверяется с реальным секретом в Vault по
 * {@code credential_ref}, взятому из реального партнёрского конфига
 * ({@link uz.mpp.partnersmpp.config.PartnerConfigLoader}) — тот же READ-
 * контракт (KV v2, {@code vault://...} -> path/property), который
 * {@code credential-issuer-service} пишет.
 *
 * <h2>Кеш — TTL 45с</h2>
 * Реальный сетевой read на КАЖДЫЙ SMPP bind добавил бы реальную задержку
 * на критическом пути ({@code authenticate} вызывается синхронно на
 * Netty event loop потоке — см. {@code SmppServerHandler::handleBind},
 * интерфейс {@link PartnerAuthenticator} остался синхронным намеренно, не
 * редизайн ради этой задачи). 45с — компромисс: bind — не submit_sm (не
 * происходит на каждое сообщение, только на (пере)подключение), но
 * ротация credential (credential-issuer-service RotateCredential) должна
 * стать эффективной без передеплоя партнёрского gateway за разумное
 * время — полная параллель с {@code renewMargin} Vault-токена и общим
 * духом остальных runtime TTL этой сессии (Redis session TTL и т.п.), не
 * найдено директивно в документах, разумное число.
 *
 * <h2>Fail-closed при недоступности Vault</h2>
 * Если Vault недоступен и НИКОГДА не было успешного чтения для этого
 * {@code system_id} — bind отклоняется ({@link
 * PartnerAuthenticator.AuthResult#reject()}), не бывает default-open
 * (см. {@code iam-service/README.md} "Fail-closed по контракту", тот же
 * принцип). Если Vault недоступен, но раньше УЖЕ был успешный read
 * (кеш просто протух по TTL) — используется последнее известное
 * значение, не полный отказ: транзиентная недоступность Vault не должна
 * рвать уже держащиеся бинды/мешать переподключению партнёра, который
 * ничего не менял. Явный компромисс между "никогда не читать протухшее"
 * и "никогда не отказывать живому партнёру из-за временной проблемы
 * инфраструктуры" — выбрано второе, задокументировано явно, не спрятано.
 */
public final class VaultAuthenticator implements PartnerAuthenticator {

    public static final Duration DEFAULT_CACHE_TTL = Duration.ofSeconds(45);

    private static final Logger LOG = Logger.getLogger(VaultAuthenticator.class.getName());

    private final Map<String, SmppBindCredential> bySystemId;
    private final VaultSecretReader vault;
    private final Duration cacheTtl;
    private final Clock clock;
    private final ConcurrentHashMap<String, CachedSecret> cache = new ConcurrentHashMap<>();

    private record CachedSecret(String value, Instant fetchedAt) {
    }

    public VaultAuthenticator(Map<String, SmppBindCredential> bySystemId, VaultSecretReader vault) {
        this(bySystemId, vault, DEFAULT_CACHE_TTL, Clock.systemUTC());
    }

    public VaultAuthenticator(Map<String, SmppBindCredential> bySystemId, VaultSecretReader vault,
                               Duration cacheTtl, Clock clock) {
        this.bySystemId = bySystemId;
        this.vault = vault;
        this.cacheTtl = cacheTtl;
        this.clock = clock;
    }

    @Override
    public AuthResult authenticate(String systemId, String password) {
        SmppBindCredential cred = bySystemId.get(systemId);
        if (cred == null) {
            // Неизвестный system_id — ни разу не обращаемся к Vault, нечего искать.
            return AuthResult.reject();
        }

        String secret;
        try {
            secret = currentSecret(systemId, cred.credentialRef());
        } catch (Exception e) {
            LOG.log(Level.WARNING, "VaultAuthenticator: чтение секрета для system_id="
                + systemId + " не удалось, нет годного кеша — отказ бинда (fail-closed)", e);
            return AuthResult.reject();
        }

        if (!constantTimeEquals(secret, password)) {
            return AuthResult.reject();
        }
        return new AuthResult(true, cred.partnerId(), cred.applicationId());
    }

    private String currentSecret(String systemId, String credentialRef) throws Exception {
        Instant now = clock.instant();
        CachedSecret cached = cache.get(systemId);
        if (cached != null && now.isBefore(cached.fetchedAt().plus(cacheTtl))) {
            return cached.value();
        }

        VaultClient.KvRef ref = VaultClient.parseCredentialRef(credentialRef);
        try {
            String value = vault.readProperty(ref.kvPath(), ref.property());
            cache.put(systemId, new CachedSecret(value, now));
            return value;
        } catch (Exception e) {
            if (cached != null) {
                // TTL истёк, но раньше был успешный read — см. докстринг класса
                // "Fail-closed при недоступности Vault".
                LOG.log(Level.WARNING, "VaultAuthenticator: свежий read для system_id="
                    + systemId + " не удался, использую протухший кеш (Vault временно недоступен)", e);
                return cached.value();
            }
            throw e;
        }
    }

    /**
     * {@link MessageDigest#isEqual(byte[], byte[])} — документированно
     * constant-time (JDK, специально для предотвращения timing-атак),
     * доступен в JDK без дополнительной зависимости — не нужно
     * реализовывать вручную, как {@code partner-rest-receiver/auth.rs
     * constant_time_eq} (Rust, там нет готового стандартного варианта).
     * Как и Rust-версия — не защищает от утечки длины пароля через тайминг
     * раннего {@code false} при несовпадении длин; тот же явно принятый
     * компромисс.
     */
    private static boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(
            a.getBytes(StandardCharsets.UTF_8),
            b.getBytes(StandardCharsets.UTF_8));
    }
}
