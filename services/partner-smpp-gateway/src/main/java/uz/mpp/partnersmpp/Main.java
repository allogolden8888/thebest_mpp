package uz.mpp.partnersmpp;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import uz.mpp.partnersmpp.admission.ExecutionControlConsumer;
import uz.mpp.partnersmpp.admission.ExecutionControlSnapshot;
import uz.mpp.partnersmpp.admission.SnapshotAdmissionGate;
import uz.mpp.partnersmpp.config.PartnerConfigStore;
import uz.mpp.partnersmpp.config.RedisUrl;
import uz.mpp.partnersmpp.grpcserver.DeliverSmServer;
import uz.mpp.partnersmpp.health.HealthServer;
import uz.mpp.partnersmpp.kafkaio.ConfigChangeConsumer;
import uz.mpp.partnersmpp.kafkaio.IncomingPublisher;
import uz.mpp.partnersmpp.registry.SessionRedisRegistry;
import uz.mpp.partnersmpp.registry.SessionHeartbeatScheduler;
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
        ExecutionControlSnapshot controlSnapshot = new ExecutionControlSnapshot();
        ExecutionControlConsumer controlConsumer = new ExecutionControlConsumer(kafkaBrokers, controlSnapshot);
        controlConsumer.start();

        // RedisUrl.buildRuntimeUrl() — не голая "redis://host:port" конкатенация
        // (см. её javadoc "найдено при реальном live-прогоне" за полным
        // разбором: без пароля SessionRedisRegistry не смог бы подключиться
        // к redis-runtime с requirepass, тем же классом находки, что несколько
        // других сервисов этого репозитория уже поймали и исправили).
        Duration heartbeatInterval = Duration.ofSeconds(Long.parseLong(env("SESSION_HEARTBEAT_INTERVAL_SECONDS", "30")));
        SessionRedisRegistry sessionRegistry = new SessionRedisRegistry(
            RedisUrl.buildRuntimeUrl(),
            env("HOSTNAME", "partner-smpp-gateway-0"),
            heartbeatInterval
        );

        // AUTH_VERIFIER_MODE=vault (по умолчанию) — реальный партнёрский
        // конфиг + реальный Vault (VaultAuthenticator, см. README "Vault-
        // аутентификация"). AUTH_VERIFIER_MODE=env — старый StaticAuthenticator,
        // один захардкоженный system_id из env vars, оставлен как явный
        // bootstrap/break-glass fallback (тот же принцип, что план требует
        // для старого статического ExternalSecret — "не удалять").
        String authVerifierMode = env("AUTH_VERIFIER_MODE", "vault");
        PartnerAuthenticator authenticator;
        VaultClient vaultClientForHealth = null;
        PartnerConfigStore partnerConfigStoreForHealth = null;
        ConfigChangeConsumer configChangeConsumer = null;

        // ChannelRegistry конструируется здесь (не в PartnerSmppServer'е,
        // как раньше) — configChangeConsumer ниже должен видеть тот же
        // экземпляр, что и SmppServerHandler, чтобы принудительное
        // разъединение архивированных партнёров (см. блок ниже) реально
        // закрывало живые каналы, а не какой-то отдельный registry.
        ChannelRegistry channelRegistry = new ChannelRegistry();

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
            // Production Readiness Review P0#4: раньше PartnerConfigLoader.fromFile
            // (PARTNER_CONFIG_PATH) читался ОДИН РАЗ при старте — новый/изменённый
            // партнёрский SMPP-бинд (partner-self-service-api -> Configuration
            // Service -> config.changes) никогда не подхватывался без ручного
            // рестарта. PartnerConfigStore заменяет это: (a) реальный bootstrap
            // из Configuration Redis (тот же источник, что config-cache-projector
            // уже проецирует, см. её README), (b) живой config.changes консьюмер
            // ниже, который на entity_type=PARTNER атомарно подменяет карту —
            // VaultAuthenticator получает Supplier, не статичный Map, так что
            // видит каждое обновление без пересоздания.
            PartnerConfigStore partnerConfigStore = new PartnerConfigStore(RedisUrl.buildConfigurationUrl());
            partnerConfigStore.bootstrap();
            if (partnerConfigStore.currentCredentials().isEmpty()) {
                System.err.println("Partner SMPP Gateway: Configuration Redis не содержит ни одного auth.type=SMPP_BIND "
                    + "приложения — ни один bind не пройдёт аутентификацию, пока не придёт первое config.changes событие");
            }

            VaultClient vaultClient = buildVaultClient();
            authenticator = new VaultAuthenticator(partnerConfigStore::currentCredentials, vaultClient);
            vaultClientForHealth = vaultClient;
            partnerConfigStoreForHealth = partnerConfigStore;

            // Судьба уже держащейся TCP-сессии на config.changes — три
            // сценария, требуемых Production Readiness Review P0#4, каждый
            // разобран отдельно:
            //
            // 1. Партнёр РОТИРУЕТ credential (self-service, status остаётся
            //    "active") при уже держащейся сессии — сессия НЕ рвётся
            //    принудительно. authenticate() и раньше вызывался только на
            //    bind (см. SmppServerHandler::handleBind), не на каждый PDU
            //    — уже держащийся бинд был легитимно аутентифицирован
            //    валидным на тот момент credential'ом, и разрыв TCP ничего
            //    не даёт с точки зрения безопасности (сокет уже открыт
            //    легитимно), только создаёт ненужный даунтайм для рутинной
            //    самообслуживаемой смены пароля. Тот же дух, что
            //    VaultAuthenticator уже применяет к своему TTL-кешу секрета
            //    (ротация должна стать эффективной за разумное время БЕЗ
            //    прерывания уже легитимно установленных соединений) —
            //    новый пароль подхватится на следующий bind (переподключение
            //    партнёра, обычно уже сегодня периодическое по инициативе
            //    самого партнёра, либо после ReadTimeoutHandler).
            // 2. Партнёр АРХИВИРУЕТСЯ/удаляется — этот случай качественно
            //    другой: архивация — явное решение оператора (или самого
            //    партнёра) прекратить доступ, не рутинная гигиена
            //    credential'а. Продолжать принимать submit_sm от уже
            //    держащейся сессии архивированного партнёра неограниченно
            //    долго (партнёр может держать сессию открытой годами через
            //    enquire_link, ReadTimeoutHandler здесь не поможет) —
            //    реальный риск (биллинг/комплаенс/абьюз) для нулевой пользы.
            //    Поэтому здесь, в отличие от (1), сессия ЗАКРЫВАЕТСЯ
            //    принудительно сразу после события — см. лямбду ниже,
            //    ChannelRegistry.sessionsForPartner. Закрытие канала
            //    проходит через тот же channelInactive -> deregister путь,
            //    что обычный unbind (SmppServerHandler), так что
            //    Runtime Redis (SessionRedisRegistry) корректно освобождается
            //    тем же unbindListener, без дублирования логики очистки
            //    здесь. StatefulSet: каждый под видит КАЖДОЕ событие (см.
            //    ConfigChangeConsumer javadoc) и закрывает ТОЛЬКО те каналы,
            //    что реально держит сам (TCP-сокет пришпилен к одному поду) —
            //    межподовая координация не нужна.
            // 3. Новое auth.type=SMPP_BIND приложение добавлено существующему
            //    активному партнёру — immutable payload события содержит весь
            //    партнёрский snapshot и атомарно заменяет его записи. Redis не
            //    перечитывается: независимый cache-projector может ещё не успеть
            //    переставить current pointer. Version fence делает replay
            //    идемпотентным и не допускает отката на старую версию.
            ChannelRegistry channelRegistryForConfigChanges = channelRegistry;
            configChangeConsumer = new ConfigChangeConsumer(
                kafkaBrokers,
                // Уникальная группа на каждый запуск ЭТОГО пода — см.
                // ConfigChangeConsumer javadoc "Каждый под — свой независимый
                // consumer group" за полным обоснованием (StatefulSet с
                // несколькими репликами, каждая должна видеть КАЖДОЕ событие,
                // не получать свою партицию общей группы).
                "partner-smpp-gateway-config-changes-" + env("HOSTNAME", "partner-smpp-gateway-0") + "-" + System.nanoTime(),
                event -> {
                    boolean changed = partnerConfigStore.applyEvent(
                        event.getEntityId(),
                        event.getVersion(),
                        event.getStatus(),
                        event.getPayloadJson().toByteArray()
                    );
                    String effectivePartnerStatus = partnerConfigStore.partnerStatus(event.getEntityId());
                    if (changed && !"active".equals(effectivePartnerStatus)) {
                        forceDisconnectPartner(channelRegistryForConfigChanges, event.getEntityId(), effectivePartnerStatus);
                    }
                }
            );
            configChangeConsumer.start();
            configChangeConsumer.awaitInitialReplay(Duration.ofSeconds(Long.parseLong(
                env("CONFIG_INITIAL_REPLAY_TIMEOUT_SECONDS", "60")
            )));
        }

        SessionHeartbeatScheduler heartbeatScheduler = new SessionHeartbeatScheduler(
            channelRegistry,
            sessionRegistry,
            heartbeatInterval
        );
        String endpoint = env("HOSTNAME", "partner-smpp-gateway-0") + ":" + env("SMPP_PORT", "2775");
        PartnerSmppServer smppServer = new PartnerSmppServer(
            authenticator,
            publisher::publish,
            Double.parseDouble(env("RATE_LIMIT_TPS", "300")),
            new SnapshotAdmissionGate(controlSnapshot),
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

        heartbeatScheduler.start();

        Map<String, java.util.concurrent.Callable<Void>> checks = new HashMap<>();
        checks.put("execution_control", () -> {
            if (!controlSnapshot.isReady()) {
                throw new IllegalStateException("full execution.control snapshot with GLOBAL sentinel is not ready");
            }
            return null;
        });
        if (vaultClientForHealth != null) {
            VaultClient vaultForCheck = vaultClientForHealth;
            // Живая проверка (PartnerConfigStore::currentCredentials), не
            // замороженный на старте снапшот — раньше /readyz "partner_config"
            // навсегда оставался бы красным для пода, который стартовал до
            // публикации первого партнёра, даже после того, как config.changes
            // догнал бы состояние; теперь отражает текущую живую карту.
            PartnerConfigStore storeForCheck = partnerConfigStoreForHealth;
            checks.put("vault", () -> {
                vaultForCheck.ping();
                return null;
            });
            checks.put("partner_config", () -> {
                if (storeForCheck.currentCredentials().isEmpty()) {
                    throw new IllegalStateException("ни одного auth.type=SMPP_BIND приложения не загружено из Configuration Redis");
                }
                return null;
            });
            ConfigChangeConsumer configForCheck = configChangeConsumer;
            checks.put("config_changes", () -> {
                if (configForCheck == null || !configForCheck.isHealthy()) {
                    throw new IllegalStateException("config.changes consumer не готов или застрял на неприменимом событии");
                }
                return null;
            });
        }
        health.setDependencyChecks(checks);

        health.setReady(true);

        ConfigChangeConsumer configChangeConsumerForShutdown = configChangeConsumer;
        PartnerConfigStore partnerConfigStoreForShutdown = partnerConfigStoreForHealth;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            grpcServer.shutdown();
            smppServer.stop();
            heartbeatScheduler.close();
            controlConsumer.close();
            sessionRegistry.close();
            publisher.close();
            if (configChangeConsumerForShutdown != null) {
                configChangeConsumerForShutdown.close();
            }
            if (partnerConfigStoreForShutdown != null) {
                partnerConfigStoreForShutdown.close();
            }
            health.stop();
        }));

        grpcServer.awaitTermination();
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }

    /**
     * Партнёр стал неактивным ({@code status != "active"}, обычно
     * {@code "archived"}) — принудительно закрывает КАЖДУЮ живую SMPP-сессию
     * этого партнёра, которую держит ЭТОТ под (см. развёрнутое обоснование
     * в вызывающем коде выше, "1./2./3."). {@code Channel.close()} на Netty
     * канале асинхронно триггерит стандартный
     * {@code SmppServerHandler::channelInactive} -> {@code deregister} ->
     * {@code unbindListener} путь — тот же путь, что обычный клиентский
     * {@code UNBIND}, так что {@code ChannelRegistry}/Runtime Redis
     * ({@code SessionRedisRegistry}) корректно освобождаются без
     * дублирования той логики здесь.
     */
    private static void forceDisconnectPartner(ChannelRegistry channelRegistry, String partnerId, String status) {
        var sessions = channelRegistry.sessionsForPartner(partnerId);
        if (sessions.isEmpty()) {
            return;
        }
        System.out.println("Partner SMPP Gateway: partner_id=" + partnerId + " status=" + status
            + " — принудительно закрываем " + sessions.size() + " живую SMPP-сессию(и) на этом поде (config.changes)");
        for (ChannelRegistry.PartnerSession s : sessions) {
            s.session().channel().close();
        }
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
