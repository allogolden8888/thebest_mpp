package uz.mpp.delivery;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.Map;

/**
 * {@code fetch_message_context} (service_internal_methods.md §1.8) —
 * {@code msgctx:{message_id}} HASH в Runtime Redis (`data_infrastructure_spec.md`
 * §284): {@code body, sender, msisdn, encoding, partner_id, segment_count, channel}.
 * Той же природы, что {@code RedisMessageContextStore} в policy-service —
 * компилируется против настоящего Lettuce-клиента, live не проверено.
 *
 * MEDIUM находка кодревью: {@code fetch} раньше открывала новое TCP-соединение
 * и делала полный Redis handshake на КАЖДЫЙ вызов ({@code client.connect()}
 * внутри try-with-resources, закрывалось сразу же) — при целевых 20 000 TPS
 * это соединение-на-сообщение, а не пул/переиспользование. {@link StatefulRedisConnection}
 * у Lettuce документированно потокобезопасно и рассчитано на переиспользование
 * из многих потоков одновременно (мультиплексирование поверх одного
 * TCP-соединения) — открывается один раз в конструкторе, живёт всё время
 * жизни сервиса, закрывается в {@link #close()}.
 */
public final class MessageContextStore {

    public record MessageContext(String body, String sender, String msisdn, String encoding) {
    }

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;

    public MessageContextStore(String redisUrl) {
        this.client = RedisClient.create(redisUrl);
        this.connection = client.connect();
    }

    public MessageContext fetch(String messageId) {
        RedisCommands<String, String> commands = connection.sync();
        Map<String, String> fields = commands.hgetall("msgctx:" + messageId);
        if (fields.isEmpty()) {
            return null;
        }
        return new MessageContext(
            fields.get("body"),
            fields.get("sender"),
            fields.get("msisdn"),
            fields.getOrDefault("encoding", "GSM7"));
    }

    public void close() {
        connection.close();
        client.shutdown();
    }
}
