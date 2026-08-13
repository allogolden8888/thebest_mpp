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

/// Реальная находка (нагрузочный прогон + прямое измерение на реальном
/// SMSC): раньше capacity сообщений-bucket'а == самому rate_limit_tps —
/// партнёр, какое-то время не отправлявший ничего, копил токенов ровно на
/// 1 секунду трафика и мог потратить их практически мгновенно, разом (в
/// пределах ЛЮБОГО 1-секундного окна возможно до `rate_limit_tps +
/// capacity` сообщений — с capacity=rate это давало до 2x номинального
/// TPS). SMSC реально видел burst до ~440/200мс при номинальных ~65/200мс
/// (CV=1.05-1.06 на двух независимых прогонах). MESSAGE_BURST_HEADROOM_FACTOR
/// ограничивает разрешённый избыток над номиналом 15% от rate_limit_tps —
/// партнёру остаётся право на небольшую неровность отправки, но не хватит
/// устроить burst, который рискует поймать throttling на реальном
/// SMPP-канале оператора. `refill_per_sec` НЕ уменьшается — средняя
/// пропускная способность в долгосрочном периоде остаётся точно
/// rate_limit_tps, меняется только максимально допустимый мгновенный избыток.
const MESSAGE_BURST_HEADROOM_FACTOR: f64 = 0.15;

impl Bucket {
    /// Для `auth_attempt_buckets` — capacity == refill_per_sec, БЕЗ урезания
    /// (см. AUTH_ATTEMPT_HEADROOM_FACTOR и комментарий у `auth_attempt_buckets`
    /// — тот burst-режим осознанно шире и не связан с находкой про SMSC).
    fn new_at(rate_limit_tps: u32, now: Instant) -> Self {
        let rate = rate_limit_tps as f64;
        Self { capacity: rate, tokens: rate, refill_per_sec: rate, last_refill: now, consumed_since_sync: 0 }
    }

    /// Для реального сообщения-bucket'а (`buckets`, не auth-попытки) — см.
    /// `MESSAGE_BURST_HEADROOM_FACTOR`. Минимум 1.0 — иначе для низкого
    /// rate_limit_tps (например 3) capacity вышла бы меньше одного полного
    /// токена, и партнёр не смог бы отправить НИ ОДНОГО сообщения никогда.
    fn new_message_bucket_at(rate_limit_tps: u32, now: Instant) -> Self {
        let rate = rate_limit_tps as f64;
        let capacity = (rate * MESSAGE_BURST_HEADROOM_FACTOR).max(1.0);
        Self { capacity, tokens: capacity, refill_per_sec: rate, last_refill: now, consumed_since_sync: 0 }
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
    /// Исправление HIGH находки кодревью: `check_rate_limit` в документированном
    /// порядке (§1.1) идёт ПОСЛЕ `authenticate_partner`, значит запрос с
    /// неверным/подобранным API-ключом никогда не доходит до `buckets` выше —
    /// `(partner_id, application_id)` не секретны (обычные заголовки), поэтому
    /// атакующий, знающий/угадавший валидную пару, мог слать неограниченное
    /// число попыток подбора ключа в секунду без всякого throttling.
    /// ОТДЕЛЬНЫЙ bucket (не тот же самый `buckets`) — принципиально: если бы
    /// это был один общий bucket, атакующий, тратящий токены на угадывание,
    /// съедал бы бюджет легитимного партнёра по той же паре. Ёмкость —
    /// `rate_limit_tps * AUTH_ATTEMPT_HEADROOM_FACTOR`, СОЗНАТЕЛЬНО шире, чем
    /// сам `rate_limit_tps`: если бы ёмкость совпадала один в один, легитимный
    /// партнёр, идущий ровно по своему лимиту, исчерпывал бы ОБА bucket'а
    /// одновременно на каждом запросе, и рутинное превышение легитимного
    /// трафика стало бы неотличимо (по коду ошибки) от блокировки перебора
    /// ключа — задел в 4x означает: обычный трафик в пределах своего лимита
    /// никогда не задевает эту проверку вообще (см. `error_response_parts`:
    /// `AuthRateLimited` — отдельный код от `RateLimited`), а подбор ключа
    /// всё равно жёстко ограничен (в 4 раза выше легитимного лимита партнёра,
    /// не безгранично, как было до находки).
    auth_attempt_buckets: Mutex<HashMap<(String, String), Bucket>>,
}

/// См. комментарий у `auth_attempt_buckets`.
const AUTH_ATTEMPT_HEADROOM_FACTOR: u32 = 4;

impl Default for RateLimiter {
    fn default() -> Self {
        Self { buckets: Mutex::new(HashMap::new()), auth_attempt_buckets: Mutex::new(HashMap::new()) }
    }
}

impl RateLimiter {
    pub fn try_consume(&self, partner_id: &str, application_id: &str, rate_limit_tps: u32) -> bool {
        self.try_consume_at(partner_id, application_id, rate_limit_tps, Instant::now())
    }

    fn try_consume_at(&self, partner_id: &str, application_id: &str, rate_limit_tps: u32, now: Instant) -> bool {
        let key = (partner_id.to_string(), application_id.to_string());
        let mut buckets = self.buckets.lock().expect("rate limiter mutex poisoned");
        let bucket = buckets.entry(key).or_insert_with(|| Bucket::new_message_bucket_at(rate_limit_tps, now));
        bucket.try_consume_at(now)
    }

    /// `check_rate_limit`, вызванный ДО `authenticate_partner` — throttling
    /// самой попытки аутентификации (успешной или нет), независимый bucket,
    /// см. комментарий у поля `auth_attempt_buckets`.
    pub fn try_consume_auth_attempt(&self, partner_id: &str, application_id: &str, rate_limit_tps: u32) -> bool {
        self.try_consume_auth_attempt_at(partner_id, application_id, rate_limit_tps, Instant::now())
    }

    fn try_consume_auth_attempt_at(&self, partner_id: &str, application_id: &str, rate_limit_tps: u32, now: Instant) -> bool {
        let key = (partner_id.to_string(), application_id.to_string());
        let capacity = rate_limit_tps.saturating_mul(AUTH_ATTEMPT_HEADROOM_FACTOR);
        let mut buckets = self.auth_attempt_buckets.lock().expect("rate limiter mutex poisoned");
        let bucket = buckets.entry(key).or_insert_with(|| Bucket::new_at(capacity, now));
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
        // rate=20 -> capacity = 20 * MESSAGE_BURST_HEADROOM_FACTOR(0.15) = 3.
        for i in 0..3 {
            assert!(limiter.try_consume_at("p1", "a1", 20, now), "сообщение {i} должно пройти (в пределах capacity=3)");
        }
        assert!(!limiter.try_consume_at("p1", "a1", 20, now), "4-е сообщение в тот же момент должно throttle-иться");
    }

    #[test]
    fn refills_over_time() {
        let limiter = RateLimiter::default();
        let now = Instant::now();
        // rate=40 -> capacity=6, но refill_per_sec ОСТАЁТСЯ полным rate (40) —
        // урезана только мгновенная ёмкость всплеска, не средняя пропускная способность.
        for _ in 0..6 {
            assert!(limiter.try_consume_at("p1", "a1", 40, now));
        }
        assert!(!limiter.try_consume_at("p1", "a1", 40, now), "bucket пуст сразу после исчерпания capacity=6");

        let later = now + Duration::from_millis(100); // refill_per_sec=40 * 0.1с = 4 токена
        for _ in 0..4 {
            assert!(limiter.try_consume_at("p1", "a1", 40, later));
        }
        assert!(
            !limiter.try_consume_at("p1", "a1", 40, later),
            "только 4 токена накопилось за 100мс при refill_per_sec=40 (сам rate не урезан, урезана только capacity)"
        );
    }

    #[test]
    fn message_bucket_capacity_is_15_percent_of_rate_not_full_rate() {
        // Прямая проверка находки нагрузочного прогона: раньше capacity ==
        // rate_limit_tps (партнёр мог мгновенно потратить целую секунду
        // накопленного трафика). Теперь — MESSAGE_BURST_HEADROOM_FACTOR.
        let limiter = RateLimiter::default();
        let now = Instant::now();
        // rate=300 (реальный rate_limit_tps click_uz_main) -> capacity=45.
        for i in 0..45 {
            assert!(limiter.try_consume_at("p1", "a1", 300, now), "токен {i} из 45 (=300*0.15) должен быть доступен");
        }
        assert!(!limiter.try_consume_at("p1", "a1", 300, now), "46-е сообщение мгновенно уже не должно проходить — старое поведение (capacity=300) пропустило бы");
    }

    #[test]
    fn message_bucket_capacity_floor_is_one_token_for_low_rate_partners() {
        // Без пола партнёр с rate_limit_tps=3 получил бы capacity=0.45 —
        // токен никогда не набрался бы до 1.0, партнёр не смог бы отправить
        // НИ ОДНОГО сообщения. capacity=max(rate*0.15, 1.0) — пол в 1 токен.
        let limiter = RateLimiter::default();
        let now = Instant::now();
        assert!(limiter.try_consume_at("p1", "a1", 3, now), "низкий rate_limit_tps всё равно должен пропускать хотя бы 1 сообщение мгновенно");
        assert!(!limiter.try_consume_at("p1", "a1", 3, now), "capacity=1 (пол) исчерпан после первого сообщения");
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
        // rate=20 -> capacity=3, с запасом хватает на 2 последовательных consume для p1/a1.
        limiter.try_consume_at("p1", "a1", 20, now);
        limiter.try_consume_at("p1", "a1", 20, now);
        limiter.try_consume_at("p2", "a1", 20, now);

        let drained = limiter.drain_consumed();
        assert_eq!(drained.get(&("p1".to_string(), "a1".to_string())), Some(&2));
        assert_eq!(drained.get(&("p2".to_string(), "a1".to_string())), Some(&1));

        let second_drain = limiter.drain_consumed();
        assert!(second_drain.is_empty(), "после drain счётчики должны обнулиться, пустой поток сообщений не публикует нулевые записи");
    }

    #[test]
    fn auth_attempt_bucket_is_independent_from_message_bucket() {
        let limiter = RateLimiter::default();
        let now = Instant::now();
        // Ёмкость auth_attempt bucket = rate_limit_tps * AUTH_ATTEMPT_HEADROOM_FACTOR (4) = 8.
        for i in 0..8 {
            assert!(limiter.try_consume_auth_attempt_at("p1", "a1", 2, now), "попытка {i} должна пройти (в пределах headroom-ёмкости)");
        }
        assert!(!limiter.try_consume_auth_attempt_at("p1", "a1", 2, now), "auth_attempt bucket должен быть исчерпан после headroom-ёмкости");
        // Обычный message-bucket (self.buckets, не auth_attempt_buckets) —
        // отдельный пул токенов, полностью полон несмотря на исчерпанный auth bucket.
        assert!(limiter.try_consume_at("p1", "a1", 2, now), "message bucket не должен быть затронут исчерпанием auth_attempt bucket");
    }

    #[test]
    fn legitimate_traffic_within_configured_tps_never_trips_auth_attempt_bucket() {
        // Партнёр, идущий РОВНО по своему rate_limit_tps (не выше) — не
        // должен НИКОГДА получить AuthRateLimited вместо обычного успеха —
        // headroom (4x) должен полностью покрывать любой легитимный паттерн,
        // ограниченный собственным rate_limit_tps message-bucket'ом.
        let limiter = RateLimiter::default();
        let now = Instant::now();
        // tps=40 -> message bucket capacity=6 (40*0.15), с запасом хватает на 5 итераций подряд.
        let tps = 40;
        for i in 0..5 {
            assert!(limiter.try_consume_auth_attempt_at("p1", "a1", tps, now), "легитимная попытка {i} не должна быть отклонена auth_attempt bucket'ом");
            assert!(limiter.try_consume_at("p1", "a1", tps, now), "легитимное сообщение {i} должно пройти message bucket");
        }
        // message bucket capacity=6, 5 уже потрачено — 6-е должно ещё пройти, 7-е уже нет.
        assert!(limiter.try_consume_at("p1", "a1", tps, now));
        assert!(!limiter.try_consume_at("p1", "a1", tps, now));
    }

    #[test]
    fn bucket_capacity_does_not_exceed_configured_rate_even_after_long_idle() {
        let limiter = RateLimiter::default();
        let now = Instant::now();
        // rate=20 -> capacity=3.
        limiter.try_consume_at("p1", "a1", 20, now);
        let much_later = now + Duration::from_secs(3600);
        // Не должно накопить 3600*20 токенов — capacity ограничивает bucket до 3,
        // сколько бы времени партнёр ни простаивал (см. MESSAGE_BURST_HEADROOM_FACTOR).
        assert!(limiter.try_consume_at("p1", "a1", 20, much_later));
        assert!(limiter.try_consume_at("p1", "a1", 20, much_later));
        assert!(limiter.try_consume_at("p1", "a1", 20, much_later));
        assert!(!limiter.try_consume_at("p1", "a1", 20, much_later), "capacity=3 не должен быть превышен долгим простоем");
    }
}
