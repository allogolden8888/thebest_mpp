//! Реальная находка (найдена при реализации `dlr-manager`, полный разбор —
//! `services/dlr-manager/README.md`, "Реальная находка (систематическая...)"):
//! k8s инжектит `REDIS_RUNTIME_HOST`/`REDIS_RUNTIME_PORT`/`REDIS_RUNTIME_PASSWORD`
//! дискретно (`envFrom: secretRef`), не единую `REDIS_RUNTIME_URL`, которую
//! этот сервис читал раньше — в реальном кластере он никогда бы не
//! подключился. `REDIS_RUNTIME_URL` оставлена как явный override для
//! локальной разработки/тестов.

pub fn build_redis_runtime_url() -> String {
    if let Ok(v) = std::env::var("REDIS_RUNTIME_URL") {
        if !v.is_empty() {
            return v;
        }
    }
    let host = std::env::var("REDIS_RUNTIME_HOST").unwrap_or_else(|_| "redis-runtime.mpp.svc".to_string());
    let port = std::env::var("REDIS_RUNTIME_PORT").unwrap_or_else(|_| "6379".to_string());
    match std::env::var("REDIS_RUNTIME_PASSWORD") {
        Ok(password) if !password.is_empty() => format!("redis://:{password}@{host}:{port}/0"),
        _ => format!("redis://{host}:{port}/0"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // env::set_var — глобальное состояние процесса; тесты этого модуля
    // должны выполняться последовательно, не параллельно с другими тестами,
    // трогающими те же переменные — тот же mutex-паттерн, что уже
    // используется в auth.rs для той же причины.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_env() {
        for key in ["REDIS_RUNTIME_URL", "REDIS_RUNTIME_HOST", "REDIS_RUNTIME_PORT", "REDIS_RUNTIME_PASSWORD"] {
            unsafe { std::env::remove_var(key) };
        }
    }

    #[test]
    fn composes_from_discrete_vars_with_password() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        unsafe {
            std::env::set_var("REDIS_RUNTIME_HOST", "redis.example.internal");
            std::env::set_var("REDIS_RUNTIME_PORT", "6380");
            std::env::set_var("REDIS_RUNTIME_PASSWORD", "r3d1s");
        }
        let url = build_redis_runtime_url();
        clear_env();
        assert_eq!(url, "redis://:r3d1s@redis.example.internal:6380/0");
    }

    #[test]
    fn composes_without_password() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        unsafe {
            std::env::set_var("REDIS_RUNTIME_HOST", "redis.example.internal");
        }
        let url = build_redis_runtime_url();
        clear_env();
        assert_eq!(url, "redis://redis.example.internal:6379/0");
    }

    #[test]
    fn explicit_override_takes_priority() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        unsafe {
            std::env::set_var("REDIS_RUNTIME_URL", "redis://explicit-override/0");
            std::env::set_var("REDIS_RUNTIME_HOST", "should-be-ignored");
        }
        let url = build_redis_runtime_url();
        clear_env();
        assert_eq!(url, "redis://explicit-override/0");
    }
}
