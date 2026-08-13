//! `sync_rate_limit_counters` (service_internal_methods.md §1.1) — реальный
//! `redis` крейт, не интеграционно проверенный в этом окружении (тот же
//! паттерн, что `RedisMessageContextStore` в policy-service).
//!
//! ИСПРАВЛЕНО (нагрузочный прогон 1500 TPS, см. `msgctx::write` за полным
//! разбором находки): раньше здесь было отдельное новое TCP-соединение на
//! каждый цикл синхронизации (~1с) вместо переиспользуемого — при 1с тике
//! это был не главный вклад в 578-684% CPU (тот был на хот-пасе `msgctx::write`,
//! на полной скорости запросов), но тот же класс проблемы, тот же общий
//! `MultiplexedConnection` из `AppState` устраняет его заодно.

use crate::rate_limit::RateLimiter;
use redis::aio::MultiplexedConnection;

pub async fn sync_rate_limit_counters(mut conn: MultiplexedConnection, rate_limiter: &RateLimiter) {
    let counts = rate_limiter.drain_consumed();
    if counts.is_empty() {
        return;
    }
    for ((partner_id, application_id), count) in counts {
        let key = format!("ratelimit:partner_rest_receiver:{partner_id}:{application_id}");
        if let Err(e) = redis::AsyncCommands::incr::<_, _, i64>(&mut conn, &key, count as i64).await {
            tracing::error!("не удалось синхронизировать счётчик rate limit для {key}: {e}");
        }
    }
}
