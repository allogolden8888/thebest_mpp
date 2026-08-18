package uz.mpp.partnersmpp;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import uz.mpp.partnersmpp.config.PartnerConfigLoader;
import uz.mpp.partnersmpp.grpcserver.DeliverSmServer;
import uz.mpp.partnersmpp.health.HealthServer;
import uz.mpp.partnersmpp.kafkaio.IncomingPublisher;
import uz.mpp.partnersmpp.registry.SessionRedisRegistry;
import uz.mpp.partnersmpp.server.ChannelRegistry;
import uz.mpp.partnersmpp.server.PartnerAuthenticator;
import uz.mpp.partnersmpp.server.PartnerSmppServer;
import uz.mpp.partnersmpp.server.StaticAuthenticator;
import uz.mpp.partnersmpp.server.VaultAuthenticator;
import uz.mpp.partnersmpp.vault.KubernetesAuthTokenSource;
import uz.mpp.partnersmpp.vault.StaticTokenSource;
import uz.mpp.partnersmpp.vault.TokenSource;
import uz.mpp.partnersmpp.vault.VaultClient;

import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Partner SMPP Gateway (services_specifictaion.md §2.2) — SMPP bind/unbind,
 * submit_sm, deliver_sm, partner-side query_sm, enquire_link.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        HealthServer health = new HealthServer();
        health.start();

        String kafkaBrokers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092");
        IncomingPublisher publisher = new IncomingPublisher(kafkaBrokers);

        String redisUri = "redis://" + env("REDIS_RUNTIME_HOST", "localhost") + ":" + env("REDIS_RUNTIME_PORT", "6379");
        SessionRedisRegistry sessionRegistry = new SessionRedisRegistry(redisUri, env("HOSTNAME", "partner-smpp-gateway-0"), Duration.ofSeconds(30));

        // AUTH_VERIFIER_MODE=vault (по умолчанию) — реальный партнёрский
        // конфиг + реальный Vault (VaultAuthenticator, см. README "Vault-
        // аутентификация"). AUTH_VERIFIER_MODE=env — старый StaticAuthenticator,
        // один захардкоженный system_id из env vars, оставлен как явный
        // bootstrap/break-glass fallback (тот же принцип, что план требует
        // для старого статического ExternalSecret — "не удалять").
        String authVerifierMode = env("AUTH_VERIFIER_MODE", "vault");
        PartnerAuthenticator authenticator;
        VaultClient vaultClientForHealth = null;
        Map<String, PartnerConfigLoader.SmppBindCredential> smppCredentialsForHealth = null;

        if ("env".equals(authVerifierMode)) {
            authenticator = new StaticAuthenticator(Map.of(
                env("SMPP_SYSTEM_ID", "demo_system_id"),
                new StaticAuthenticator.Credential(
                    env("SMPP_PASSWORD", "demo_password"),
                    env("SMPP_PARTNER_ID", "demo_partner"),
                    env("SMPP_APPLICATION_ID", "demo_application")
                )
            ));
        } else {
            Path partnerConfigPath = Path.of(env("PARTNER_CONFIG_PATH", "../../config_schemas/examples/partner.valid.json"));
            Map<String, PartnerConfigLoader.SmppBindCredential> smppCredentials = PartnerConfigLoader.fromFile(partnerConfigPath);
            if (smppCredentials.isEmpty()) {
                System.err.println("Partner SMPP Gateway: " + partnerConfigPath
                    + " не содержит ни одного auth.type=SMPP_BIND приложения — ни один bind не пройдёт аутентификацию, пока конфиг не обновится");
            }

            VaultClient vaultClient = buildVaultClient();
            authenticator = new VaultAuthenticator(smppCredentials, vaultClient);
            vaultClientForHealth = vaultClient;
            smppCredentialsForHealth = smppCredentials;
        }

        ChannelRegistry channelRegistry = new ChannelRegistry();
        String endpoint = env("HOSTNAME", "partner-smpp-gateway-0") + ":" + env("SMPP_PORT", "2775");
        PartnerSmppServer smppServer = new PartnerSmppServer(
            authenticator,
            publisher::publish,
            Double.parseDouble(env("RATE_LIMIT_TPS", "300")),
            channelRegistry,
            (partnerId, systemId, sessionEpoch) ->
                sessionRegistry.register(partnerId, systemId, systemId + "-" + sessionEpoch, sessionEpoch, endpoint),
            sessionRegistry::unregister
        );
        int smppPort = smppServer.start(Integer.parseInt(env("SMPP_PORT", "2775")));
        System.out.println("Partner SMPP Gateway слушает :" + smppPort);

        Server grpcServer = ServerBuilder.forPort(Integer.parseInt(env("GRPC_PORT", "9000")))
            .addService(new DeliverSmServer(channelRegistry))
            .build()
            .start();
        System.out.println("gRPC PartnerDeliverSmService слушает :" + Integer.parseInt(env("GRPC_PORT", "9000")));

        if (vaultClientForHealth != null) {
            VaultClient vaultForCheck = vaultClientForHealth;
            Map<String, PartnerConfigLoader.SmppBindCredential> credsForCheck = smppCredentialsForHealth;
            Map<String, java.util.concurrent.Callable<Void>> checks = new HashMap<>();
            checks.put("vault", () -> {
                vaultForCheck.ping();
                return null;
            });
            checks.put("partner_config", () -> {
                if (credsForCheck.isEmpty()) {
                    throw new IllegalStateException("ни одного auth.type=SMPP_BIND приложения не загружено из PARTNER_CONFIG_PATH");
                }
                return null;
            });
            health.setDependencyChecks(checks);
        }

        health.setReady(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            grpcServer.shutdown();
            smppServer.stop();
            sessionRegistry.close();
            publisher.close();
            health.stop();
        }));

        grpcServer.awaitTermination();
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }

    /**
     * {@code VAULT_TOKEN} задан -> static token (local/dev/break-glass,
     * тот же escape hatch, что сам {@code vault} CLI поддерживает).
     * Иначе -> реальный Kubernetes auth login, роль из
     * {@code VAULT_K8S_AUTH_ROLE} (по умолчанию {@code
     * partner-credential-readers} — {@code
     * infra/terraform/vault-secrets.tf
     * vault_kubernetes_auth_backend_role.partner_credential_readers},
     * общая с partner-rest-receiver). Прямое зеркало {@code
     * credential-issuer-service/cmd/credential-issuer-service/main.go
     * buildVaultTokenSource}.
     */
    private static VaultClient buildVaultClient() {
        String addr = env("VAULT_ADDR", "http://vault.vault-system.svc:8200");
        String mount = env("VAULT_KV_MOUNT", "mpp");
        HttpClient httpClient = HttpClient.newHttpClient();

        String staticToken = System.getenv("VAULT_TOKEN");
        TokenSource tokenSource;
        if (staticToken != null && !staticToken.isEmpty()) {
            tokenSource = new StaticTokenSource(staticToken);
        } else {
            String role = env("VAULT_K8S_AUTH_ROLE", "partner-credential-readers");
            Path jwtPath = Path.of(env("VAULT_K8S_JWT_PATH", "/var/run/secrets/kubernetes.io/serviceaccount/token"));
            tokenSource = new KubernetesAuthTokenSource(addr, role, jwtPath, httpClient);
        }

        return new VaultClient(addr, mount, tokenSource, httpClient);
    }
}
