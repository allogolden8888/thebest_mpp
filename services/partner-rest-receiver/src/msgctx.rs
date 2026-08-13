//! Реальная находка, обнаруженная только прогоном платформы целиком через
//! локальный docker-compose (development_plan.md 2.3/2.4), не статичным
//! чтением кода: `msgctx:{message_id}` в Runtime Redis
//! (`data_infrastructure_spec.md` §284) — единственный источник полного
//! содержимого сообщения (`body`/`sender`/`msisdn`/`encoding`) для
//! downstream-стадий (Policy, Delivery, Billing) — Kafka между стадиями
//! несёт только `message_id`/`stage_execution_id`, не сам текст. Каждая из
//! этих стадий (`policy-service::RedisMessageContextStore`,
//! `delivery-service::MessageContextStore` и т.д.) уже реализует ЧТЕНИЕ
//! этого ключа — но нигде в репозитории не было ни одной ЗАПИСИ в него.
//! Живьём это проявилось как `не удалось получить MessageContext` в
//! `policy-service` на первом же реальном сообщении, прошедшем через
//! `pipeline-engine`. Естественное место записи — этот сервис: он первым
//! видит полное содержимое сообщения и уже вычисляет `encoding`/`segment_count`
//! (см. `segmentation.rs`) для `SmsPayload`, публикуемого в Kafka — та же
//! информация, что нужна `msgctx`, просто продублированная в Redis для
//! стадий, которые не хотят таскать полный текст через каждый Kafka-топик.

use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;

pub struct MessageContext<'a> {
    pub message_id: &'a str,
    pub body: &'a str,
    pub sender_id: &'a str,
    pub msisdn: &'a str,
    pub encoding: &'a str,
    pub partner_id: &'a str,
    pub segment_count: i32,
}

fn redis_key(message_id: &str) -> String {
    format!("msgctx:{message_id}")
}

/// Best-effort, как `idempotency::claim` — Redis-недоступность здесь не
/// блокирует ingress (уже опубликовали в Kafka к моменту вызова), но
/// оставляет downstream-стадии без контекста для этого сообщения; логируется
/// как ошибка, не паникует и не влияет на HTTP-ответ партнёру.
///
/// `conn` — клон общего `MultiplexedConnection` из `AppState` (создан один
/// раз при старте), не свежее подключение на каждый вызов. Реальная находка
/// нагрузочного прогона (1500 TPS push): раньше здесь было `Client::open` +
/// `get_multiplexed_async_connection` на КАЖДЫЙ запрос — `strace -c` под
/// нагрузкой показал ~90% времени в syscall'ах на socket/connect/setsockopt/
/// close (TCP-хендшейк + Redis AUTH на каждое сообщение), а не в бизнес-логике,
/// что и объясняло 578-684% CPU этого сервиса при 1500 TPS. `Client`/новое
/// соединение здесь были осознанно упрощены как "как у redis_sync.rs", но
/// тот таск тикает раз в секунду, а этот путь — на полной скорости запросов.
/// Компромисс: `MultiplexedConnection` не переподключается сам при обрыве
/// (в отличие от `ConnectionManager`) — реcтарт Redis потребует рестарта
/// этого сервиса; для локального walking skeleton (Redis не рестартует
/// посреди прогона) это приемлемо, для прод-грейд устойчивости нужен
/// `ConnectionManager` отдельным шагом.
pub async fn write(mut conn: MultiplexedConnection, ctx: &MessageContext<'_>, ttl_seconds: u64) {
    let key = redis_key(ctx.message_id);
    let fields: [(&str, String); 7] = [
        ("body", ctx.body.to_string()),
        ("sender", ctx.sender_id.to_string()),
        ("msisdn", ctx.msisdn.to_string()),
        ("encoding", ctx.encoding.to_string()),
        ("partner_id", ctx.partner_id.to_string()),
        ("segment_count", ctx.segment_count.to_string()),
        ("channel", "SMS".to_string()),
    ];

    if let Err(e) = conn.hset_multiple::<_, _, _, ()>(&key, &fields).await {
        tracing::error!("не удалось записать msgctx {key}: {e}");
        return;
    }
    if let Err(e) = conn.expire::<_, ()>(&key, ttl_seconds as i64).await {
        tracing::error!("не удалось выставить TTL на msgctx {key}: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_redis_url() -> String {
        std::env::var("PARTNER_REST_RECEIVER_TEST_REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379/0".to_string())
    }

    async fn cleanup(url: &str, key: &str) {
        if let Ok(client) = redis::Client::open(url) {
            if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
                let _: redis::RedisResult<()> = conn.del(key).await;
            }
        }
    }

    /// Реальный round-trip против локального Redis — прямое доказательство
    /// исправления находки: после `write`, поля читаются обратно ровно теми
    /// именами, которые ожидает `policy-service::RedisMessageContextStore::fetch`
    /// (`msisdn`/`sender`/`body`) — не придуманная схема, сверено с реальным
    /// читателем.
    #[tokio::test]
    async fn write_then_hgetall_round_trips_fields_readers_expect() {
        let url = test_redis_url();
        let message_id = format!("test-msgctx-{}", uuid::Uuid::new_v4());
        let key = redis_key(&message_id);
        cleanup(&url, &key).await;

        let client = redis::Client::open(url.as_str()).expect("valid redis url");
        let mut conn = match client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(_) => return, // Redis недоступен в этой песочнице — тот же паттерн skip, что и остальные live-тесты
        };

        let ctx = MessageContext {
            message_id: &message_id,
            body: "тестовое сообщение",
            sender_id: "MPP",
            msisdn: "998901331835",
            encoding: "UCS2",
            partner_id: "click_uz",
            segment_count: 1,
        };
        write(conn.clone(), &ctx, 60).await;
        let fields: std::collections::HashMap<String, String> =
            redis::AsyncCommands::hgetall(&mut conn, &key).await.expect("hgetall should succeed after write");

        assert_eq!(fields.get("msisdn"), Some(&"998901331835".to_string()));
        assert_eq!(fields.get("sender"), Some(&"MPP".to_string()));
        assert_eq!(fields.get("body"), Some(&"тестовое сообщение".to_string()));
        assert_eq!(fields.get("encoding"), Some(&"UCS2".to_string()));
        assert_eq!(fields.get("partner_id"), Some(&"click_uz".to_string()));
        assert_eq!(fields.get("segment_count"), Some(&"1".to_string()));
        assert_eq!(fields.get("channel"), Some(&"SMS".to_string()));

        let ttl: i64 = redis::AsyncCommands::ttl(&mut conn, &key).await.expect("ttl should succeed");
        assert!(ttl > 0 && ttl <= 60, "TTL должен быть выставлен и не превышать запрошенный, получили {ttl}");

        cleanup(&url, &key).await;
    }
}
