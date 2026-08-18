//! `VaultAuthVerifier` — читающая сторона Фазы 1 (`credential-issuer-service`
//! — пишущая сторона, см. его README для полного обоснования формата
//! `credential_ref`/KV v2 путей). Раньше единственным `AuthVerifier` был
//! `EnvAuthVerifier` (`auth.rs`) — статическая, **deploy-time** связка
//! `credential_ref` → env var, которую ничто не обновляет вживую: партнёр,
//! ротировавший ключ через `credential-issuer-service::RotateCredential`,
//! оставался бы неаутентифицируемым до ручного редеплоя этого сервиса.
//! `VaultAuthVerifier` читает секрет напрямую из Vault на каждый (с учётом
//! кеша) запрос — то же значение, что видит `credential-issuer-service`
//! сразу после ротации.
//!
//! Vault HTTP API — те же две операции, что `credential-issuer-service/internal/vault/client.go`
//! (зеркалится байт-в-байт, не изобретается заново):
//! * Kubernetes auth login: `POST {addr}/v1/auth/kubernetes/login`,
//!   `{"role","jwt"}` -> `.auth.client_token`/`.auth.lease_duration`,
//!   кешируется с `RENEW_MARGIN` запасом до истечения.
//! * KV v2 read: `GET {addr}/v1/{mount}/data/{path}` + `X-Vault-Token` ->
//!   `.data.data.<property>` (двойная вложенность `data.data` — конверт KV v2
//!   поверх собственно полей).
//!
//! `parse_credential_ref` — байт-в-байт та же логика, что Go
//! `vault.ParseCredentialRef` и Python
//! `generate_external_secrets.py::vault_kv_path_and_property`: три независимые
//! реализации одного правила, должны совпадать, иначе путь записи
//! (`credential-issuer-service`) разойдётся с путём чтения (этот файл).

use crate::auth::{AuthVerifier, constant_time_eq};
use crate::partner_config::Application;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum VaultError {
    Http(reqwest::Error),
    Rejected { status: reqwest::StatusCode, body: String },
    NotFound(String),
    PropertyMissing { kv_path: String, property: String },
    InvalidCredentialRef(String),
    JwtFileUnreadable { path: String, source: std::io::Error },
    LoginRejected { status: reqwest::StatusCode, body: String },
    LoginResponseMissingToken,
}

impl std::fmt::Display for VaultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VaultError::Http(e) => write!(f, "vault: HTTP-запрос: {e}"),
            VaultError::Rejected { status, body } => write!(f, "vault: запрос отклонён, статус {status}: {body}"),
            VaultError::NotFound(path) => write!(f, "vault: путь {path} не найден"),
            VaultError::PropertyMissing { kv_path, property } => {
                write!(f, "vault: поле {property} отсутствует по пути {kv_path}")
            }
            VaultError::InvalidCredentialRef(r) => write!(f, "vault: credential_ref {r:?} не имеет формы vault://path/property"),
            VaultError::JwtFileUnreadable { path, source } => write!(f, "vault: чтение ServiceAccount JWT ({path}): {source}"),
            VaultError::LoginRejected { status, body } => write!(f, "vault: login отклонён, статус {status}: {body}"),
            VaultError::LoginResponseMissingToken => write!(f, "vault: login-ответ без auth.client_token"),
        }
    }
}

impl std::error::Error for VaultError {}

/// `vault://partners/click_uz/main/api_key` -> (`partners/click_uz/main`,
/// `api_key`) — split на ПОСЛЕДНЕМ `/`, не на первом (см. doc-комментарий
/// модуля: должно byte-for-byte совпадать с Go `ParseCredentialRef` и Python
/// `vault_kv_path_and_property`).
pub fn parse_credential_ref(credential_ref: &str) -> Result<(String, String), VaultError> {
    const PREFIX: &str = "vault://";
    let path = credential_ref
        .strip_prefix(PREFIX)
        .ok_or_else(|| VaultError::InvalidCredentialRef(credential_ref.to_string()))?;
    match path.rfind('/') {
        Some(idx) if idx > 0 && idx != path.len() - 1 => Ok((path[..idx].to_string(), path[idx + 1..].to_string())),
        _ => Err(VaultError::InvalidCredentialRef(credential_ref.to_string())),
    }
}

/// Запас до истечения токена, за который начинаем релогиниться — та же
/// величина, что Go `renewMargin` (`credential-issuer-service/internal/vault/client.go`).
const RENEW_MARGIN: Duration = Duration::from_secs(30);

struct KubernetesTokenState {
    cached_token: Option<String>,
    expires_at: Option<Instant>,
}

/// Production token source — обменивает projected ServiceAccount JWT на
/// Vault client token через `/v1/auth/kubernetes/login`, кеширует до
/// истечения с `RENEW_MARGIN` запасом. Зеркало Go `KubernetesAuthTokenSource`.
pub struct KubernetesAuthTokenSource {
    addr: String,
    role: String,
    jwt_path: String,
    http: reqwest::Client,
    state: Mutex<KubernetesTokenState>,
}

impl KubernetesAuthTokenSource {
    pub fn new(addr: String, role: String, jwt_path: String, http: reqwest::Client) -> Self {
        Self { addr, role, jwt_path, http, state: Mutex::new(KubernetesTokenState { cached_token: None, expires_at: None }) }
    }

    async fn token(&self) -> Result<String, VaultError> {
        {
            let state = self.state.lock().unwrap();
            let still_fresh = match state.expires_at {
                // checked_sub, не `-` — в тестах/на раннем старте процесса
                // Instant может быть моложе RENEW_MARGIN от эпохи процесса,
                // прямое вычитание запаниковало бы; None здесь корректно
                // читается как "уже пора релогиниться".
                Some(exp) => exp.checked_sub(RENEW_MARGIN).is_some_and(|threshold| Instant::now() < threshold),
                None => false,
            };
            if still_fresh {
                if let Some(token) = &state.cached_token {
                    return Ok(token.clone());
                }
            }
        }

        let jwt = tokio::fs::read_to_string(&self.jwt_path)
            .await
            .map_err(|source| VaultError::JwtFileUnreadable { path: self.jwt_path.clone(), source })?;

        let resp = self
            .http
            .post(format!("{}/v1/auth/kubernetes/login", self.addr))
            .json(&serde_json::json!({ "role": self.role, "jwt": jwt.trim() }))
            .send()
            .await
            .map_err(VaultError::Http)?;

        let status = resp.status();
        if status != reqwest::StatusCode::OK {
            let body = resp.text().await.unwrap_or_default();
            return Err(VaultError::LoginRejected { status, body });
        }

        #[derive(serde::Deserialize)]
        struct LoginResponse {
            auth: LoginAuth,
        }
        #[derive(serde::Deserialize)]
        struct LoginAuth {
            #[serde(default)]
            client_token: String,
            #[serde(default)]
            lease_duration: u64,
        }
        let parsed: LoginResponse = resp.json().await.map_err(VaultError::Http)?;
        if parsed.auth.client_token.is_empty() {
            return Err(VaultError::LoginResponseMissingToken);
        }

        let mut state = self.state.lock().unwrap();
        state.cached_token = Some(parsed.auth.client_token.clone());
        state.expires_at = Some(Instant::now() + Duration::from_secs(parsed.auth.lease_duration));
        Ok(parsed.auth.client_token)
    }
}

/// `TokenSource` — способ получить действующий Vault client token. Zeркало
/// Go `TokenSource` интерфейса с двумя реализациями — здесь как enum
/// (не trait object): ровно два известных варианта, dyn не нужен, `Arc`
/// вокруг enum достаточен, чтобы `VaultClient` был дешёво `Clone`.
pub enum TokenSource {
    /// Local/dev/break-glass — `VAULT_TOKEN` напрямую, тот же escape hatch,
    /// что `vault` CLI сам поддерживает через переменную окружения.
    Static(String),
    /// Production — реальный Kubernetes auth login.
    Kubernetes(KubernetesAuthTokenSource),
}

impl TokenSource {
    async fn token(&self) -> Result<String, VaultError> {
        match self {
            TokenSource::Static(token) => Ok(token.clone()),
            TokenSource::Kubernetes(source) => source.token().await,
        }
    }
}

#[derive(serde::Deserialize)]
struct Kv2Response {
    data: Kv2Data,
}
#[derive(serde::Deserialize)]
struct Kv2Data {
    data: HashMap<String, serde_json::Value>,
}

/// Тонкая обёртка над Vault KV v2 HTTP API, mount `"mpp"` по умолчанию
/// (`infra/terraform/vault-secrets.tf` `vault_mount.mpp`, тот же mount, что
/// `infra/secrets/generate_external_secrets.py VAULT_KV_MOUNT` и
/// `credential-issuer-service`). `Clone` дёшев — `reqwest::Client` внутри
/// себя `Arc`, `token_source` тоже завёрнут в `Arc`, так что читающий
/// health-check и `VaultAuthVerifier` могут держать независимые клоны без
/// дублирования пула соединений/кеша токена.
#[derive(Clone)]
pub struct VaultClient {
    addr: String,
    mount: String,
    token_source: Arc<TokenSource>,
    http: reqwest::Client,
}

impl VaultClient {
    pub fn new(addr: impl Into<String>, mount: impl Into<String>, token_source: TokenSource, http: reqwest::Client) -> Self {
        Self { addr: addr.into(), mount: mount.into(), token_source: Arc::new(token_source), http }
    }

    /// `buildVaultTokenSource` (Go `main.go`) — `VAULT_TOKEN` задан ->
    /// статический токен, иначе реальный Kubernetes auth login с ролью
    /// `VAULT_K8S_AUTH_ROLE` (дефолт `partner-credential-readers` —
    /// `infra/terraform/vault-secrets.tf` `vault_kubernetes_auth_backend_role.partner_credential_readers`,
    /// роль уже привязана к ServiceAccount'ам `partner-rest-receiver` и
    /// `partner-smpp-gateway`) и JWT-путём `VAULT_K8S_JWT_PATH`.
    pub fn from_env() -> Self {
        let addr = std::env::var("VAULT_ADDR").unwrap_or_else(|_| "http://vault.vault-system.svc:8200".to_string());
        let mount = std::env::var("VAULT_MOUNT").unwrap_or_else(|_| "mpp".to_string());
        let http = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("reqwest::Client builds with static config");

        let token_source = if let Ok(token) = std::env::var("VAULT_TOKEN") {
            tracing::info!("VAULT_TOKEN задан — статический токен (local/dev/break-glass), не Kubernetes auth login");
            TokenSource::Static(token)
        } else {
            let role = std::env::var("VAULT_K8S_AUTH_ROLE").unwrap_or_else(|_| "partner-credential-readers".to_string());
            let jwt_path =
                std::env::var("VAULT_K8S_JWT_PATH").unwrap_or_else(|_| "/var/run/secrets/kubernetes.io/serviceaccount/token".to_string());
            TokenSource::Kubernetes(KubernetesAuthTokenSource::new(addr.clone(), role, jwt_path, http.clone()))
        };

        Self::new(addr, mount, token_source, http)
    }

    fn data_url(&self, kv_path: &str) -> String {
        format!("{}/v1/{}/data/{}", self.addr, self.mount, kv_path)
    }

    pub async fn read_kv2_property(&self, kv_path: &str, property: &str) -> Result<String, VaultError> {
        let token = self.token_source.token().await?;
        let resp = self.http.get(self.data_url(kv_path)).header("X-Vault-Token", token).send().await.map_err(VaultError::Http)?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(VaultError::NotFound(kv_path.to_string()));
        }
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(VaultError::Rejected { status, body });
        }

        let parsed: Kv2Response = resp.json().await.map_err(VaultError::Http)?;
        parsed
            .data
            .data
            .get(property)
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| VaultError::PropertyMissing { kv_path: kv_path.to_string(), property: property.to_string() })
    }

    /// `GET /v1/sys/health` — без токена (одна из немногих Vault-ручек,
    /// открытых без аутентификации, специально для health-проб). 200
    /// (unsealed+active) и 429 (unsealed+standby) оба считаются здоровыми —
    /// standby-реплика всё ещё обслуживает трафик через HA-прокси в реальном
    /// кластере.
    pub async fn ping(&self) -> Result<(), VaultError> {
        let resp = self.http.get(format!("{}/v1/sys/health", self.addr)).send().await.map_err(VaultError::Http)?;
        let status = resp.status();
        if status == reqwest::StatusCode::OK || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            Ok(())
        } else {
            Err(VaultError::Rejected { status, body: String::new() })
        }
    }
}

/// TTL кеша прочитанного секрета — на порядок больше, чем один HTTP-запрос,
/// на порядок меньше, чем "заметная задержка распространения ротации".
/// 30с — тот же порядок величины, что `RENEW_MARGIN` у Kubernetes-токена:
/// после `RotateCredential` в `credential-issuer-service` новый ключ
/// становится действующим здесь максимум через один TTL-интервал, а Vault
/// не получает по сетевому чтению на КАЖДЫЙ входящий REST-запрос (единственная
/// точка входа всех REST-партнёров — при заметном TPS это было бы and реальной
/// добавленной задержкой на каждое сообщение, и лишней нагрузкой на Vault).
/// НЕ настраивается через env var в этом срезе — если понадобится другое
/// значение для разных окружений, это тривиальное дополнение, не
/// архитектурное решение.
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(30);

struct CacheEntry {
    value: String,
    fetched_at: Instant,
}

/// `VaultAuthVerifier` — дефолтный `AuthVerifier` (см. `main.rs`,
/// `AUTH_VERIFIER_MODE`). Кеш — простой `Mutex<HashMap>`, НЕ ограничен по
/// размеру и не вытесняет записи явно (только перезаписывает при повторном
/// чтении): для MVP-скоупа (один партнёр, разумное число приложений) это не
/// проблема — см. README "Что НЕ реализовано". Fail-closed: на ошибку
/// чтения из Vault (недоступность, 404, отсутствующее поле, битый
/// credential_ref) `verify` возвращает `false`, никогда не пропускает
/// запрос по умолчанию — тот же принцип, что `iam-service::CheckPermission`
/// (см. его README, "Fail-closed по контракту"): сервис, проверяющий
/// credentials, не должен по умолчанию открываться, когда его зависимость
/// недоступна. Кеш-хит (значение моложе TTL) НЕ делает сетевой запрос вообще
/// — значит устойчив к кратковременной недоступности Vault до истечения TTL;
/// только реальный cache miss (первое обращение к этому credential_ref или
/// истёкший TTL) при недоступном Vault приводит к отказу.
pub struct VaultAuthVerifier {
    client: VaultClient,
    cache: Mutex<HashMap<String, CacheEntry>>,
    ttl: Duration,
}

impl VaultAuthVerifier {
    pub fn new(client: VaultClient) -> Self {
        Self::with_ttl(client, DEFAULT_CACHE_TTL)
    }

    pub fn with_ttl(client: VaultClient, ttl: Duration) -> Self {
        Self { client, cache: Mutex::new(HashMap::new()), ttl }
    }

    async fn expected_secret(&self, credential_ref: &str) -> Result<String, VaultError> {
        if let Some(entry) = self.cache.lock().unwrap().get(credential_ref) {
            if entry.fetched_at.elapsed() < self.ttl {
                return Ok(entry.value.clone());
            }
        }

        let (kv_path, property) = parse_credential_ref(credential_ref)?;
        let value = self.client.read_kv2_property(&kv_path, &property).await?;

        self.cache.lock().unwrap().insert(credential_ref.to_string(), CacheEntry { value: value.clone(), fetched_at: Instant::now() });
        Ok(value)
    }
}

impl AuthVerifier for VaultAuthVerifier {
    fn verify<'a>(&'a self, application: &'a Application, provided_key: &'a str) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            match self.expected_secret(&application.auth.credential_ref).await {
                Ok(expected) => constant_time_eq(expected.as_bytes(), provided_key.as_bytes()),
                Err(err) => {
                    tracing::error!(
                        "Vault: чтение credential_ref={} для {} не удалось: {err} — доступ отклонён (fail closed)",
                        application.auth.credential_ref,
                        application.application_id
                    );
                    false
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partner_config::AuthConfig;
    use axum::extract::{Path, State};
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn app(credential_ref: &str) -> Application {
        Application {
            application_id: "app1".into(),
            display_name: "Test App".into(),
            auth: AuthConfig { auth_type: "API_KEY".into(), credential_ref: credential_ref.into() },
            ip_allowlist: vec![],
            rate_limit_tps: 100,
            allowed_channels: vec!["SMS".into()],
        }
    }

    // ---- parse_credential_ref — тот же вектор случаев, что Go TestParseCredentialRef ----

    #[test]
    fn parses_simple_ref() {
        let (path, prop) = parse_credential_ref("vault://partners/click_uz/main/api_key").unwrap();
        assert_eq!(path, "partners/click_uz/main");
        assert_eq!(prop, "api_key");
    }

    #[test]
    fn parses_ref_with_extra_path_segments_splitting_on_last_slash() {
        let (path, prop) = parse_credential_ref("vault://partners/click_uz/main/nested/api_key").unwrap();
        assert_eq!(path, "partners/click_uz/main/nested");
        assert_eq!(prop, "api_key");
    }

    #[test]
    fn rejects_missing_vault_prefix() {
        assert!(parse_credential_ref("not-a-vault-ref").is_err());
    }

    #[test]
    fn rejects_empty_path_after_prefix() {
        assert!(parse_credential_ref("vault://").is_err());
    }

    #[test]
    fn rejects_ref_without_any_slash() {
        assert!(parse_credential_ref("vault://no-slash-at-all").is_err());
    }

    #[test]
    fn rejects_trailing_slash_empty_property() {
        assert!(parse_credential_ref("vault://partners/click_uz/main/").is_err());
    }

    // ---- реальный локальный Vault dev-server (127.0.0.1:8200, root token) ----

    /// Тот же обход, что Go `testAddr` — реальный I/O там, где окружение
    /// позволяет, честно пропущено (`return None`), где нет.
    async fn real_vault_client() -> Option<VaultClient> {
        let addr = std::env::var("VAULT_TEST_ADDR").unwrap_or_else(|_| "http://127.0.0.1:8200".to_string());
        let http = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().unwrap();
        let client = VaultClient::new(addr, "mpp", TokenSource::Static("root".to_string()), http);
        if client.ping().await.is_err() {
            eprintln!("локальный Vault недоступен — пропуск (см. README, `vault server -dev -dev-root-token-id=root`)");
            return None;
        }
        Some(client)
    }

    fn unique_kv_path(label: &str) -> String {
        format!("partners/test-partner/{label}-{}", uuid::Uuid::new_v4())
    }

    async fn seed_secret(http_root: &reqwest::Client, addr: &str, kv_path: &str, property: &str, value: &str) {
        // Прямой write через root token — независимый от read-пути, который
        // тестируется (тот же принцип, что Go-тесты верифицируют write через
        // отдельный ReadKV2, здесь наоборот: пишем напрямую, читаем через
        // код, который тестируем).
        let url = format!("{addr}/v1/mpp/data/{kv_path}");
        let resp = http_root
            .post(&url)
            .header("X-Vault-Token", "root")
            .json(&serde_json::json!({ "data": { property: value } }))
            .send()
            .await
            .expect("write запрос к локальному Vault");
        assert!(resp.status().is_success(), "seed write не удался: {}", resp.status());
    }

    #[tokio::test]
    async fn successful_read_and_verify_against_real_vault() {
        let Some(client) = real_vault_client().await else { return };
        let addr = "http://127.0.0.1:8200".to_string();
        let http_root = reqwest::Client::new();
        let kv_path = unique_kv_path("verify-ok");
        seed_secret(&http_root, &addr, &kv_path, "api_key", "real-secret-value").await;

        let verifier = VaultAuthVerifier::new(client);
        let application = app(&format!("vault://{kv_path}/api_key"));

        assert!(verifier.verify(&application, "real-secret-value").await);
        assert!(!verifier.verify(&application, "wrong-value").await);
    }

    #[tokio::test]
    async fn missing_path_fails_closed_not_panics() {
        let Some(client) = real_vault_client().await else { return };
        let verifier = VaultAuthVerifier::new(client);
        let application = app("vault://partners/test-partner/does-not-exist/api_key");
        assert!(!verifier.verify(&application, "anything").await);
    }

    #[tokio::test]
    async fn missing_property_on_existing_path_fails_closed() {
        let Some(client) = real_vault_client().await else { return };
        let addr = "http://127.0.0.1:8200".to_string();
        let http_root = reqwest::Client::new();
        let kv_path = unique_kv_path("missing-prop");
        seed_secret(&http_root, &addr, &kv_path, "other_field", "irrelevant").await;

        let verifier = VaultAuthVerifier::new(client);
        let application = app(&format!("vault://{kv_path}/api_key"));
        assert!(!verifier.verify(&application, "anything").await);
    }

    #[tokio::test]
    async fn malformed_credential_ref_fails_closed_not_panics() {
        let Some(client) = real_vault_client().await else { return };
        let verifier = VaultAuthVerifier::new(client);
        let application = app("not-a-vault-ref-at-all");
        assert!(!verifier.verify(&application, "anything").await);
    }

    #[tokio::test]
    async fn cache_hit_serves_stale_value_without_rereading_and_ttl_expiry_picks_up_rotation() {
        let Some(client) = real_vault_client().await else { return };
        let addr = "http://127.0.0.1:8200".to_string();
        let http_root = reqwest::Client::new();
        let kv_path = unique_kv_path("rotation");
        seed_secret(&http_root, &addr, &kv_path, "api_key", "value-v1").await;

        // Короткий TTL — тест не должен спать 30с.
        let verifier = VaultAuthVerifier::with_ttl(client, Duration::from_millis(200));
        let application = app(&format!("vault://{kv_path}/api_key"));

        // Первый verify — реальный read, кеширует value-v1.
        assert!(verifier.verify(&application, "value-v1").await);

        // Ротация "за спиной" кеша — прямой overwrite в Vault, не через verifier.
        seed_secret(&http_root, &addr, &kv_path, "api_key", "value-v2").await;

        // Всё ещё внутри TTL — verify должен использовать КЕШИРОВАННОЕ value-v1,
        // не видеть value-v2 (доказательство: старый ключ всё ещё проходит,
        // новый ключ ещё не работает) — ни одного нового сетевого чтения.
        assert!(verifier.verify(&application, "value-v1").await, "в пределах TTL должен использоваться кеш (value-v1), не свежее чтение");
        assert!(!verifier.verify(&application, "value-v2").await, "в пределах TTL новое значение ещё не должно быть видно");

        tokio::time::sleep(Duration::from_millis(300)).await;

        // TTL истёк — теперь настоящий cache miss, свежее чтение должно
        // увидеть value-v2 (ротация подхватилась), старое значение больше не работает.
        assert!(verifier.verify(&application, "value-v2").await, "после истечения TTL должно быть видно новое значение");
        assert!(!verifier.verify(&application, "value-v1").await, "после истечения TTL старое значение больше не должно проходить");
    }

    // ---- fail-closed без реального Vault вообще (unreachable addr) ----

    #[tokio::test]
    async fn unreachable_vault_fails_closed_on_genuine_cache_miss() {
        let http = reqwest::Client::builder().timeout(Duration::from_millis(300)).build().unwrap();
        // Порт 1 — заведомо ничего не слушает, тот же трюк, что Go
        // TestPingRejectsUnreachableAddr.
        let client = VaultClient::new("http://127.0.0.1:1", "mpp", TokenSource::Static("root".into()), http);
        let verifier = VaultAuthVerifier::new(client);
        let application = app("vault://partners/click_uz/main/api_key");
        assert!(!verifier.verify(&application, "anything").await);
    }

    // ---- локальный fake Vault (axum) — счётчик запросов доказывает кеш-хит не делает сетевой вызов ----

    #[derive(Clone)]
    struct FakeKvServerState {
        reads: Arc<AtomicUsize>,
        value: Arc<Mutex<String>>,
    }

    async fn fake_kv2_read(State(state): State<FakeKvServerState>, Path(_kv_path): Path<String>) -> Json<serde_json::Value> {
        state.reads.fetch_add(1, Ordering::SeqCst);
        let value = state.value.lock().unwrap().clone();
        Json(serde_json::json!({ "data": { "data": { "api_key": value } } }))
    }

    async fn spawn_fake_kv_server(initial_value: &str) -> (String, Arc<AtomicUsize>, Arc<Mutex<String>>) {
        let reads = Arc::new(AtomicUsize::new(0));
        let value = Arc::new(Mutex::new(initial_value.to_string()));
        let state = FakeKvServerState { reads: reads.clone(), value: value.clone() };
        let router = Router::new().route("/v1/mpp/data/*kv_path", get(fake_kv2_read)).with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (format!("http://{addr}"), reads, value)
    }

    #[tokio::test]
    async fn cache_hit_against_fake_server_makes_zero_additional_requests() {
        let (addr, reads, _value) = spawn_fake_kv_server("secret-abc").await;
        let http = reqwest::Client::new();
        let client = VaultClient::new(addr, "mpp", TokenSource::Static("root".into()), http);
        let verifier = VaultAuthVerifier::new(client); // дефолтный TTL=30с, точно не истечёт за тест
        let application = app("vault://partners/click_uz/main/api_key");

        assert!(verifier.verify(&application, "secret-abc").await);
        assert_eq!(reads.load(Ordering::SeqCst), 1, "первый verify должен сходить в Vault ровно один раз");

        for _ in 0..5 {
            assert!(verifier.verify(&application, "secret-abc").await);
        }
        assert_eq!(reads.load(Ordering::SeqCst), 1, "последующие verify в пределах TTL не должны делать НИ ОДНОГО дополнительного сетевого запроса");
    }

    #[tokio::test]
    async fn ttl_expiry_triggers_exactly_one_fresh_request() {
        let (addr, reads, value) = spawn_fake_kv_server("secret-v1").await;
        let http = reqwest::Client::new();
        let client = VaultClient::new(addr, "mpp", TokenSource::Static("root".into()), http);
        let verifier = VaultAuthVerifier::with_ttl(client, Duration::from_millis(150));
        let application = app("vault://partners/click_uz/main/api_key");

        assert!(verifier.verify(&application, "secret-v1").await);
        assert_eq!(reads.load(Ordering::SeqCst), 1);

        *value.lock().unwrap() = "secret-v2".to_string();
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(verifier.verify(&application, "secret-v2").await, "после истечения TTL должно быть подхвачено новое значение");
        assert_eq!(reads.load(Ordering::SeqCst), 2, "истечение TTL должно вызвать РОВНО один новый запрос");
    }

    // ---- Kubernetes auth login — тот же класс обхода, что Go TestKubernetesAuthTokenSourceLoginAndCache ----
    // (реального k8s ServiceAccount JWT/kubernetes auth method здесь нет —
    // локальная песочница, не k8s-кластер; реальный HTTP round-trip против
    // настоящего локального сервера, не мок транспорта).

    #[derive(Clone)]
    struct FakeLoginServerState {
        calls: Arc<AtomicUsize>,
        lease_duration_secs: u64,
        expected_role: &'static str,
        expected_jwt: &'static str,
    }

    async fn fake_login_handler(State(state): State<FakeLoginServerState>, Json(body): Json<serde_json::Value>) -> Json<serde_json::Value> {
        state.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(body["role"].as_str(), Some(state.expected_role));
        assert_eq!(body["jwt"].as_str(), Some(state.expected_jwt));
        Json(serde_json::json!({ "auth": { "client_token": "vault-token-from-login", "lease_duration": state.lease_duration_secs } }))
    }

    async fn spawn_fake_login_server(lease_duration_secs: u64) -> (String, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = FakeLoginServerState { calls: calls.clone(), lease_duration_secs, expected_role: "credential-issuer-service-test", expected_jwt: "fake-service-account-jwt" };
        let router = Router::new().route("/v1/auth/kubernetes/login", post(fake_login_handler)).with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (format!("http://{addr}"), calls)
    }

    async fn write_fake_jwt() -> String {
        let dir = std::env::temp_dir().join(format!("vault-auth-test-jwt-{}", uuid::Uuid::new_v4()));
        tokio::fs::write(&dir, "fake-service-account-jwt\n").await.unwrap();
        dir.to_string_lossy().to_string()
    }

    #[tokio::test]
    async fn kubernetes_login_caches_token_second_call_does_not_relogin() {
        let (addr, calls) = spawn_fake_login_server(3600).await;
        let jwt_path = write_fake_jwt().await;
        let source =
            KubernetesAuthTokenSource::new(addr, "credential-issuer-service-test".to_string(), jwt_path, reqwest::Client::new());

        let token = source.token().await.expect("первый login");
        assert_eq!(token, "vault-token-from-login");

        let token_again = source.token().await.expect("второй вызов — из кеша");
        assert_eq!(token_again, "vault-token-from-login");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "второй вызов до истечения TTL не должен снова логиниться");
    }

    #[tokio::test]
    async fn kubernetes_login_relogs_after_expiry() {
        // lease_duration=1с — тривиально истечёт быстрее RENEW_MARGIN (30с),
        // второй вызов должен релогиниться.
        let (addr, calls) = spawn_fake_login_server(1).await;
        let jwt_path = write_fake_jwt().await;
        let source =
            KubernetesAuthTokenSource::new(addr, "credential-issuer-service-test".to_string(), jwt_path, reqwest::Client::new());

        source.token().await.expect("первый login");
        source.token().await.expect("второй вызов — токен формально ещё жив 1с, но внутри RENEW_MARGIN, должен релогиниться");
        assert_eq!(calls.load(Ordering::SeqCst), 2, "оба вызова должны были реально залогиниться — токен с TTL=1с всегда внутри 30с RENEW_MARGIN");
    }

    #[tokio::test]
    async fn kubernetes_login_missing_jwt_file_returns_error_not_panic() {
        let source = KubernetesAuthTokenSource::new(
            "http://127.0.0.1:1".to_string(),
            "role".to_string(),
            "/does/not/exist/anywhere".to_string(),
            reqwest::Client::new(),
        );
        assert!(source.token().await.is_err());
    }

    #[tokio::test]
    async fn kubernetes_login_rejected_returns_error_not_panic() {
        let router = Router::new().route(
            "/v1/auth/kubernetes/login",
            post(|| async { (axum::http::StatusCode::FORBIDDEN, Json(serde_json::json!({ "errors": ["permission denied"] }))) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let jwt_path = write_fake_jwt().await;
        let source = KubernetesAuthTokenSource::new(format!("http://{addr}"), "role".to_string(), jwt_path, reqwest::Client::new());
        assert!(source.token().await.is_err());
    }

    // ---- ping ----

    #[tokio::test]
    async fn ping_rejects_unreachable_addr() {
        let http = reqwest::Client::builder().timeout(Duration::from_millis(300)).build().unwrap();
        let client = VaultClient::new("http://127.0.0.1:1", "mpp", TokenSource::Static("root".into()), http);
        assert!(client.ping().await.is_err());
    }

    #[tokio::test]
    async fn ping_succeeds_against_real_vault_if_available() {
        let Some(client) = real_vault_client().await else { return };
        assert!(client.ping().await.is_ok());
    }
}
