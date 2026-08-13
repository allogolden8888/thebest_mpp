//! Закрывает HIGH находку кодревью (PART 2, partner-rest-receiver #2):
//! ничего в REST-контракте не давало партнёру механизм идемпотентности, а
//! `generate_message_id` всегда генерировал свежий `UUID v4` независимо от
//! содержимого запроса — единственный downstream dedup-ключ (`charge_id`
//! в Billing = `stage_execution_id`) выводится из ЭТОГО message_id и не мог
//! обнаружить, что два разных `message_id` — один и тот же логический
//! партнёрский submit. Конкретный сценарий из находки: TCP-соединение
//! партнёра рвётся после успешной публикации и 202, партнёрский retry
//! повторяет идентичный payload — без этого модуля он трактуется как
//! совершенно новое сообщение.
//!
//! Опциональный заголовок `X-Idempotency-Key` (партнёр сам генерирует,
//! стабильный на повтор одного логического запроса) — тот же паттерн, что
//! Stripe/большинство платёжных REST API используют для этого класса
//! проблемы. Атомарный `SET NX EX` в Runtime Redis (тот же Redis, что уже
//! используется в этом сервисе для `sync_rate_limit_counters`) — не
//! двухфазный claim/record, как `SubmitIdempotencyStore` в delivery-service:
//! там между claim и записью исхода происходил реальный side effect (SMPP
//! submit), здесь `message_id`/`trace_id` генерируются чистой функцией без
//! I/O ДО обращения к Redis, поэтому claim и запись значения — один и тот же
//! атомарный вызов.

use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedIds {
    pub message_id: String,
    pub trace_id: String,
}

#[derive(Debug, PartialEq)]
pub enum ClaimOutcome {
    /// Первая попытка с этим ключом — вызывающая сторона публикует как обычно.
    Won,
    /// Ключ уже был использован раньше — переиспользуем тот же message_id/trace_id,
    /// НЕ публикуем повторно в Kafka.
    AlreadyClaimed(ClaimedIds),
    /// Redis недоступен/ошибка — намеренно НЕ блокирует ingress: идемпотентность
    /// здесь best-effort поверх уже существующего (без неё) поведения, а не
    /// новое жёсткое требование доступности для входной точки всей платформы.
    /// Вызывающая сторона трактует это как Won (публикует как обычно).
    Unavailable,
}

fn redis_key(partner_id: &str, application_id: &str, idempotency_key: &str) -> String {
    format!("idempotency:partner_rest_receiver:{partner_id}:{application_id}:{idempotency_key}")
}

fn encode(ids: &ClaimedIds) -> String {
    format!("{}|{}", ids.message_id, ids.trace_id)
}

fn decode(value: &str) -> Option<ClaimedIds> {
    let (message_id, trace_id) = value.split_once('|')?;
    Some(ClaimedIds { message_id: message_id.to_string(), trace_id: trace_id.to_string() })
}

/// `conn` — клон общего `MultiplexedConnection` из `AppState`, не свежее
/// подключение на каждый вызов — см. `msgctx::write` за полным разбором
/// находки (нагрузочный прогон 1500 TPS, `strace -c`: ~90% времени в
/// socket/connect/close вместо бизнес-логики).
pub async fn claim(
    mut conn: MultiplexedConnection,
    partner_id: &str,
    application_id: &str,
    idempotency_key: &str,
    ids: &ClaimedIds,
    ttl_seconds: u64,
) -> ClaimOutcome {
    let key = redis_key(partner_id, application_id, idempotency_key);
    let opts = redis::SetOptions::default()
        .conditional_set(redis::ExistenceCheck::NX)
        .with_expiration(redis::SetExpiry::EX(ttl_seconds));

    let set_result: redis::RedisResult<Option<String>> = conn.set_options(&key, encode(ids), opts).await;
    match set_result {
        Ok(Some(_)) => ClaimOutcome::Won,
        Ok(None) => {
            // NX отклонил — ключ уже занят, читаем ранее сохранённое значение.
            let existing: redis::RedisResult<Option<String>> = conn.get(&key).await;
            match existing {
                Ok(Some(value)) => match decode(&value) {
                    Some(ids) => ClaimOutcome::AlreadyClaimed(ids),
                    None => {
                        tracing::error!("idempotency key {key} содержит нераспарсиваемое значение {value:?}");
                        ClaimOutcome::Unavailable
                    }
                },
                // Ключ истёк по TTL между NX и GET (гонка) — трактуем как Unavailable,
                // не как AlreadyClaimed с пустыми данными: вызывающая сторона
                // публикует как обычно (Won-подобное поведение), безопасное направление.
                Ok(None) => ClaimOutcome::Unavailable,
                Err(e) => {
                    tracing::error!("redis GET не удался после NX-конфликта для idempotency key {key}: {e}");
                    ClaimOutcome::Unavailable
                }
            }
        }
        Err(e) => {
            tracing::error!("redis SET NX не удался для idempotency key {key}: {e}");
            ClaimOutcome::Unavailable
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trips() {
        let ids = ClaimedIds { message_id: "m1".into(), trace_id: "t1".into() };
        assert_eq!(decode(&encode(&ids)), Some(ids));
    }

    #[test]
    fn decode_rejects_malformed_value() {
        assert_eq!(decode("no-delimiter-here"), None);
    }

    #[test]
    fn redis_key_is_scoped_by_partner_application_and_key() {
        let k1 = redis_key("click_uz", "click_uz_main", "order-42");
        let k2 = redis_key("click_uz", "click_uz_marketing", "order-42");
        assert_ne!(k1, k2, "одинаковый idempotency_key у разных application_id не должен коллизировать");
    }

    fn test_redis_url() -> String {
        std::env::var("PARTNER_REST_RECEIVER_TEST_REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379/0".to_string())
    }

    /// `None` — тот же skip-паттерн, что и раньше: Redis недоступен в этой песочнице.
    async fn test_conn() -> Option<MultiplexedConnection> {
        redis::Client::open(test_redis_url().as_str()).ok()?.get_multiplexed_async_connection().await.ok()
    }

    async fn cleanup(conn: &mut MultiplexedConnection, key: &str) {
        let _: redis::RedisResult<()> = conn.del(key).await;
    }

    /// Реальный round-trip против локального Redis — прямое доказательство
    /// исправления HIGH находки: первая попытка выигрывает claim и публикует,
    /// повтор с ТЕМ ЖЕ ключом получает ТОТ ЖЕ message_id/trace_id и не должен
    /// публиковать повторно (см. вызов в `http.rs::handle_send_message`).
    #[tokio::test]
    async fn second_claim_with_same_key_reuses_first_ids_not_a_fresh_claim() {
        let Some(mut conn) = test_conn().await else { return };
        let idempotency_key = format!("test-key-{}", uuid::Uuid::new_v4());
        let key = redis_key("click_uz", "click_uz_main", &idempotency_key);
        cleanup(&mut conn, &key).await;

        let first_ids = ClaimedIds { message_id: "m-first".into(), trace_id: "t-first".into() };
        let first = claim(conn.clone(), "click_uz", "click_uz_main", &idempotency_key, &first_ids, 60).await;
        assert_eq!(first, ClaimOutcome::Won, "первая попытка должна выиграть claim");

        // Вторая попытка передаёт ДРУГИЕ вновь сгенерированные ids (как было
        // бы при настоящем повторном HTTP-запросе — generate_message_id()
        // вызывается заново до обращения к idempotency store) — retry должен
        // получить ПЕРВЫЕ ids обратно, не свои собственные.
        let second_ids = ClaimedIds { message_id: "m-second".into(), trace_id: "t-second".into() };
        let second = claim(conn.clone(), "click_uz", "click_uz_main", &idempotency_key, &second_ids, 60).await;
        assert_eq!(second, ClaimOutcome::AlreadyClaimed(first_ids));

        cleanup(&mut conn, &key).await;
    }

    #[tokio::test]
    async fn different_idempotency_keys_get_independent_claims() {
        let Some(mut conn) = test_conn().await else { return };
        let key_a = format!("test-key-a-{}", uuid::Uuid::new_v4());
        let key_b = format!("test-key-b-{}", uuid::Uuid::new_v4());
        let redis_key_a = redis_key("click_uz", "click_uz_main", &key_a);
        let redis_key_b = redis_key("click_uz", "click_uz_main", &key_b);
        cleanup(&mut conn, &redis_key_a).await;
        cleanup(&mut conn, &redis_key_b).await;

        let ids_a = ClaimedIds { message_id: "m-a".into(), trace_id: "t-a".into() };
        let ids_b = ClaimedIds { message_id: "m-b".into(), trace_id: "t-b".into() };
        assert_eq!(claim(conn.clone(), "click_uz", "click_uz_main", &key_a, &ids_a, 60).await, ClaimOutcome::Won);
        assert_eq!(claim(conn.clone(), "click_uz", "click_uz_main", &key_b, &ids_b, 60).await, ClaimOutcome::Won, "независимый ключ не должен быть затронут первым claim'ом");

        cleanup(&mut conn, &redis_key_a).await;
        cleanup(&mut conn, &redis_key_b).await;
    }
}
