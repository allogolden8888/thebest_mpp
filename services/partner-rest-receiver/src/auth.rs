//! `authenticate_partner` (service_internal_methods.md §1.1). `partner.schema.json`
//! только хранит `credential_ref` — ссылку на секрет (Vault path), не сам секрет
//! ("конфиг не хранит credential в открытом виде"). Реального Vault-клиента в
//! этом срезе нет (как и `RedisMessageContextStore`/`MessageContextStore` у
//! Policy Service, компилируется/тестируется реализация без сети) — вместо
//! этого `AuthVerifier` — трейт, реальная реализация (`EnvAuthVerifier`) читает
//! ожидаемый ключ из переменной окружения, тем же способом, каким уже
//! реально устроены 5 платформенных секретов в этом репозитории
//! (`k8s/generate_manifests.py SECRET_DEPENDENCIES` → `envFrom` → переменные
//! окружения) — не выдумано заново, framework этого репозитория для секретов
//! именно такой. Имя переменной строится из `credential_ref` детерминированно.

use crate::partner_config::Application;

pub trait AuthVerifier: Send + Sync {
    fn verify(&self, application: &Application, provided_key: &str) -> bool;
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
    fn verify(&self, application: &Application, provided_key: &str) -> bool {
        let env_var = credential_ref_to_env_var(&application.auth.credential_ref);
        match std::env::var(&env_var) {
            Ok(expected) => constant_time_eq(expected.as_bytes(), provided_key.as_bytes()),
            Err(_) => {
                tracing::error!("переменная окружения {env_var} для {} не задана — доступ отклонён", application.application_id);
                false
            }
        }
    }
}

/// Сравнение без short-circuit по длине совпавшего префикса — не защищает от
/// сравнения через `==`/timing side-channel в общем случае (сеть/GC/JIT
/// добавляют куда больший шум), но исключает самый дешёвый класс тайминг-атаки:
/// побайтовое сравнение, останавливающееся на первом несовпадении.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
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

    #[test]
    fn correct_key_verifies() {
        let env_var = credential_ref_to_env_var("vault://partners/test/verify_ok/api_key");
        unsafe { std::env::set_var(&env_var, "secret123") };
        let result = EnvAuthVerifier.verify(&app("vault://partners/test/verify_ok/api_key"), "secret123");
        unsafe { std::env::remove_var(&env_var) };
        assert!(result);
    }

    #[test]
    fn wrong_key_rejected() {
        let env_var = credential_ref_to_env_var("vault://partners/test/verify_wrong/api_key");
        unsafe { std::env::set_var(&env_var, "secret123") };
        let result = EnvAuthVerifier.verify(&app("vault://partners/test/verify_wrong/api_key"), "wrong-key");
        unsafe { std::env::remove_var(&env_var) };
        assert!(!result);
    }

    #[test]
    fn missing_env_var_rejected_not_panic() {
        let result = EnvAuthVerifier.verify(&app("vault://partners/test/never_set/api_key"), "anything");
        assert!(!result);
    }

    #[test]
    fn constant_time_eq_rejects_different_lengths() {
        assert!(!constant_time_eq(b"short", b"much longer string"));
    }
}
