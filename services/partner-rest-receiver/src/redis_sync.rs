//! `sync_rate_limit_counters` (service_internal_methods.md §1.1) — реальный
//! `redis` крейт, не интеграционно проверенный в этом окружении (тот же
//! паттерн, что `RedisMessageContextStore` в policy-service). Известное
//! упрощение, тот же класс, что уже задокументирован как отложенный в
//! `billing-service/README.md` ("нет пулинга соединений"): новое TCP-соединение
//! на каждый цикл синхронизации (~1с), а не переиспользуемое мультиплексированное
//! — не сделано в этом срезе ради объёма, тот же компромисс, тот же реальный
//! (не выдуманный) API вызовов.

use crate::rate_limit::RateLimiter;

pub async fn sync_rate_limit_counters(redis_url: &str, rate_limiter: &RateLimiter) {
    let counts = rate_limiter.drain_consumed();
    if counts.is_empty() {
        return;
    }
    let client = match redis::Client::open(redis_url) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("невалидный REDIS_RUNTIME_URL: {e}");
            return;
        }
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("не удалось подключиться к Runtime Redis для синхронизации rate limit: {e}");
            return;
        }
    };
    for ((partner_id, application_id), count) in counts {
        let key = format!("ratelimit:partner_rest_receiver:{partner_id}:{application_id}");
        if let Err(e) = redis::AsyncCommands::incr::<_, _, i64>(&mut conn, &key, count as i64).await {
            tracing::error!("не удалось синхронизировать счётчик rate limit для {key}: {e}");
        }
    }
}
