//! `check_rate_limit` + `sync_rate_limit_counters` (service_internal_methods.md
//! §1.1) — локальный in-memory token bucket на пару `(partner_id, application_id)`,
//! периодическая синхронизация агрегированного счётчика в Runtime Redis (~1с),
//! НЕ Redis round-trip на каждое сообщение (`service_io_contracts.md` §1.1,
//! cross-service note `service_io_contracts.md:609` — тот же паттерн у Partner
//! SMPP Gateway).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

struct Bucket {
    capacity: f64,
    tokens: f64,
    refill_per_sec: f64,
    last_refill: Instant,
    /// Сколько сообщений принято с последней синхронизации в Runtime Redis —
    /// `drain_consumed` читает и обнуляет это поле раз в ~1с.
    consumed_since_sync: u64,
}

impl Bucket {
    fn new_at(rate_limit_tps: u32, now: Instant) -> Self {
        let rate = rate_limit_tps as f64;
        Self { capacity: rate, tokens: rate, refill_per_sec: rate, last_refill: now, consumed_since_sync: 0 }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill).as_secs_f64();
        if elapsed <= 0.0 {
            return;
        }
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        self.last_refill = now;
    }

    fn try_consume_at(&mut self, now: Instant) -> bool {
        self.refill(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            self.consumed_since_sync += 1;
            true
        } else {
            false
        }
    }
}

/// Ключ — `(partner_id, application_id)`, как задокументировано
/// `service_internal_methods.md` §1.1 (`check_rate_limit`: `partner_id`,
/// `application_id`, локальный in-memory token bucket).
pub struct RateLimiter {
    buckets: Mutex<HashMap<(String, String), Bucket>>,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self { buckets: Mutex::new(HashMap::new()) }
    }
}

impl RateLimiter {
    pub fn try_consume(&self, partner_id: &str, application_id: &str, rate_limit_tps: u32) -> bool {
        self.try_consume_at(partner_id, application_id, rate_limit_tps, Instant::now())
    }

    fn try_consume_at(&self, partner_id: &str, application_id: &str, rate_limit_tps: u32, now: Instant) -> bool {
        let key = (partner_id.to_string(), application_id.to_string());
        let mut buckets = self.buckets.lock().expect("rate limiter mutex poisoned");
        let bucket = buckets.entry(key).or_insert_with(|| Bucket::new_at(rate_limit_tps, now));
        bucket.try_consume_at(now)
    }

    /// `sync_rate_limit_counters` — вызывается по таймеру (~1с), возвращает
    /// снапшот "сколько сообщений принято по каждой паре с прошлого вызова"
    /// для публикации в Runtime Redis, и обнуляет счётчики. Реальная запись
    /// в Redis — в `kafka_io.rs`/`main.rs` (I/O), эта функция — чистая логика
    /// накопления/дренажа, тестируется без сети.
    pub fn drain_consumed(&self) -> HashMap<(String, String), u64> {
        let mut buckets = self.buckets.lock().expect("rate limiter mutex poisoned");
        buckets
            .iter_mut()
            .filter(|(_, b)| b.consumed_since_sync > 0)
            .map(|(k, b)| {
                let count = b.consumed_since_sync;
                b.consumed_since_sync = 0;
                (k.clone(), count)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn allows_up_to_capacity_then_throttles() {
        let limiter = RateLimiter::default();
        let now = Instant::now();
        for i in 0..3 {
            assert!(limiter.try_consume_at("p1", "a1", 3, now), "сообщение {i} должно пройти (в пределах capacity=3)");
        }
        assert!(!limiter.try_consume_at("p1", "a1", 3, now), "4-е сообщение в тот же момент должно throttle-иться");
    }

    #[test]
    fn refills_over_time() {
        let limiter = RateLimiter::default();
        let now = Instant::now();
        for _ in 0..5 {
            limiter.try_consume_at("p1", "a1", 5, now);
        }
        assert!(!limiter.try_consume_at("p1", "a1", 5, now), "bucket пуст сразу после исчерпания");

        let later = now + Duration::from_millis(500); // 5 tps * 0.5s = 2.5 токена
        assert!(limiter.try_consume_at("p1", "a1", 5, later));
        assert!(limiter.try_consume_at("p1", "a1", 5, later));
        assert!(!limiter.try_consume_at("p1", "a1", 5, later), "только 2 токена накопилось за 500мс при 5 tps");
    }

    #[test]
    fn different_partner_application_pairs_have_independent_buckets() {
        let limiter = RateLimiter::default();
        let now = Instant::now();
        assert!(limiter.try_consume_at("p1", "a1", 1, now));
        assert!(!limiter.try_consume_at("p1", "a1", 1, now), "p1/a1 исчерпан");
        assert!(limiter.try_consume_at("p1", "a2", 1, now), "p1/a2 — независимый bucket, не должен быть затронут");
        assert!(limiter.try_consume_at("p2", "a1", 1, now), "p2/a1 — независимый bucket");
    }

    #[test]
    fn drain_consumed_returns_and_resets_counters() {
        let limiter = RateLimiter::default();
        let now = Instant::now();
        limiter.try_consume_at("p1", "a1", 10, now);
        limiter.try_consume_at("p1", "a1", 10, now);
        limiter.try_consume_at("p2", "a1", 10, now);

        let drained = limiter.drain_consumed();
        assert_eq!(drained.get(&("p1".to_string(), "a1".to_string())), Some(&2));
        assert_eq!(drained.get(&("p2".to_string(), "a1".to_string())), Some(&1));

        let second_drain = limiter.drain_consumed();
        assert!(second_drain.is_empty(), "после drain счётчики должны обнулиться, пустой поток сообщений не публикует нулевые записи");
    }

    #[test]
    fn bucket_capacity_does_not_exceed_configured_rate_even_after_long_idle() {
        let limiter = RateLimiter::default();
        let now = Instant::now();
        limiter.try_consume_at("p1", "a1", 3, now);
        let much_later = now + Duration::from_secs(3600);
        // Не должно накопить 3600*3 токенов — capacity ограничивает bucket до 3.
        assert!(limiter.try_consume_at("p1", "a1", 3, much_later));
        assert!(limiter.try_consume_at("p1", "a1", 3, much_later));
        assert!(limiter.try_consume_at("p1", "a1", 3, much_later));
        assert!(!limiter.try_consume_at("p1", "a1", 3, much_later), "capacity=3 не должен быть превышен долгим простоем");
    }
}
