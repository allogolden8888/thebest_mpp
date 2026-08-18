//! `authenticate_partner` (service_internal_methods.md §1.1). `partner.schema.json`
//! только хранит `credential_ref` — ссылку на секрет (Vault path), не сам секрет
//! ("конфиг не хранит credential в открытом виде"). Два реальных `AuthVerifier`:
//! `EnvAuthVerifier` (ниже, читает ожидаемый ключ из переменной окружения, тем
//! же способом, каким уже реально устроены 5 платформенных секретов в этом
//! репозитории — `k8s/generate_manifests.py SECRET_DEPENDENCIES` → `envFrom` →
//! переменные окружения — не выдумано заново) и `VaultAuthVerifier`
//! (`vault_auth.rs`, реальный Vault-клиент с TTL-кешем, теперь дефолт —
//! см. `main.rs`/`AUTH_VERIFIER_MODE`). `EnvAuthVerifier` остаётся как явный
//! bootstrap/break-glass fallback, не удалён. Имя переменной строится из
//! `credential_ref` детерминированно.
//!
//! `verify` возвращает `Pin<Box<dyn Future<Output = bool> + Send + '_>>`, не
//! просто `bool` — `VaultAuthVerifier` делает реальный сетевой I/O (Vault
//! read на cache miss), а трейт по-прежнему используется как `Box<dyn
//! AuthVerifier>` в `AppState`, так что async fn в трейте (не dyn-совместимый
//! без ручного бокса или крейта `async-trait`, который этот сервис сознательно
//! не добавляет — см. `Cargo.toml`) должен быть развёрнут вручную.
//! `EnvAuthVerifier::verify` не делает I/O — тело `async move` просто
//! немедленно возвращает готовое значение, ноль реальной асинхронности.

use crate::partner_config::Application;
use std::future::Future;
use std::pin::Pin;

pub trait AuthVerifier: Send + Sync {
    fn verify<'a>(&'a self, application: &'a Application, provided_key: &'a str) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>>;
}

/// `credential_ref` вида `vault://partners/click_uz/main/api_key` -> переменная
/// окружения `PARTNER_CRED__PARTNERS__CLICK_UZ__MAIN__API_KEY` (не-alnum -> `_`,
/// в верхний регистр, с префиксом `PARTNER_CRED_`).
pub fn credential_ref_to_env_var(credential_ref: &str) -> String {
    let sanitized: String = credential_ref
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_uppercase() } else { '_' })
        .collect();
    format!("PARTNER_CRED_{sanitized}")
}

pub struct EnvAuthVerifier;

impl AuthVerifier for EnvAuthVerifier {
    fn verify<'a>(&'a self, application: &'a Application, provided_key: &'a str) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            let env_var = credential_ref_to_env_var(&application.auth.credential_ref);
            match std::env::var(&env_var) {
                Ok(expected) => constant_time_eq(expected.as_bytes(), provided_key.as_bytes()),
                Err(_) => {
                    tracing::error!("переменная окружения {env_var} для {} не задана — доступ отклонён", application.application_id);
                    false
                }
            }
        })
    }
}

/// Сравнение без short-circuit по длине совпавшего префикса — не защищает от
/// сравнения через `==`/timing side-channel в общем случае (сеть/GC/JIT
/// добавляют куда больший шум), но исключает самый дешёвый класс тайминг-атаки:
/// побайтовое сравнение, останавливающееся на первом несовпадении.
/// `pub(crate)` — переиспользуется `vault_auth::VaultAuthVerifier` (то же
/// сравнение секрета, полученного из Vault, не только из env var).
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partner_config::AuthConfig;

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

    #[test]
    fn credential_ref_maps_to_deterministic_env_var_name() {
        let env_var = credential_ref_to_env_var("vault://partners/click_uz/main/api_key");
        assert_eq!(env_var, "PARTNER_CRED_VAULT___PARTNERS_CLICK_UZ_MAIN_API_KEY");
    }

    #[tokio::test]
    async fn correct_key_verifies() {
        let env_var = credential_ref_to_env_var("vault://partners/test/verify_ok/api_key");
        unsafe { std::env::set_var(&env_var, "secret123") };
        let result = EnvAuthVerifier.verify(&app("vault://partners/test/verify_ok/api_key"), "secret123").await;
        unsafe { std::env::remove_var(&env_var) };
        assert!(result);
    }

    #[tokio::test]
    async fn wrong_key_rejected() {
        let env_var = credential_ref_to_env_var("vault://partners/test/verify_wrong/api_key");
        unsafe { std::env::set_var(&env_var, "secret123") };
        let result = EnvAuthVerifier.verify(&app("vault://partners/test/verify_wrong/api_key"), "wrong-key").await;
        unsafe { std::env::remove_var(&env_var) };
        assert!(!result);
    }

    #[tokio::test]
    async fn missing_env_var_rejected_not_panic() {
        let result = EnvAuthVerifier.verify(&app("vault://partners/test/never_set/api_key"), "anything").await;
        assert!(!result);
    }

    #[test]
    fn constant_time_eq_rejects_different_lengths() {
        assert!(!constant_time_eq(b"short", b"much longer string"));
    }
}
