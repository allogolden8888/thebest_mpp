package uz.mpp.partnersmpp.server;

import io.netty.channel.Channel;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Держатель активных bound-сессий этого инстанса — register_session
 * (service_internal_methods.md §1.2), in-memory зеркало того, что также
 * пишется в Runtime Redis (`smpp:partner_session:{partner_id}:{system_id}`,
 * data_infrastructure_spec.md §2.1) для доступа Partner Notification Service
 * через instance-addressed gRPC (handle_deliver_sm_command).
 */
public final class ChannelRegistry {

    public record ActiveSession(Channel channel, long sessionEpoch) {
    }

    /** {@code system_id} + сессия — используется {@link #sessionsForPartner(String)}, где ключ-строка сама по себе не нужна вызывающей стороне. */
    public record PartnerSession(String systemId, ActiveSession session) {
    }

    private final Map<String, ActiveSession> sessions = new ConcurrentHashMap<>();
    private final AtomicLong epochSource = new AtomicLong();

    private static String key(String partnerId, String systemId) {
        return partnerId + ":" + systemId;
    }

    private static String partnerIdOf(String key) {
        return key.substring(0, key.indexOf(':'));
    }

    private static String systemIdOf(String key) {
        return key.substring(key.indexOf(':') + 1);
    }

    /** register — вызывается при успешном bind; возвращает новый session_epoch. */
    public long register(String partnerId, String systemId, Channel channel) {
        long epoch = epochSource.incrementAndGet();
        sessions.put(key(partnerId, systemId), new ActiveSession(channel, epoch));
        return epoch;
    }

    public void unregister(String partnerId, String systemId) {
        sessions.remove(key(partnerId, systemId));
    }

    /** Удаляет запись, только если канал совпадает — на случай, если новая сессия уже успела зарегистрироваться. */
    public void unregisterIfSameChannel(String partnerId, String systemId, Channel channel) {
        sessions.computeIfPresent(key(partnerId, systemId), (k, v) -> v.channel() == channel ? null : v);
    }

    public ActiveSession lookup(String partnerId, String systemId) {
        return sessions.get(key(partnerId, systemId));
    }

    /**
     * Снапшот ВСЕХ живых сессий данного партнёра на ЭТОМ инстансе — не знает
     * заранее ни один {@code system_id} (партнёр может держать несколько
     * bind'ов, по одному на приложение). Используется на {@code
     * entity_type=PARTNER config.changes} со статусом, отличным от {@code
     * "active"} ({@code Main.java}, обоснование см. её javadoc "Архивация —
     * принудительное разъединение") — принудительно закрывает ровно те
     * каналы, что реально держит этот под, остальные реплики StatefulSet'а
     * делают то же самое независимо для своих сессий (каждый под — свой
     * consumer group, см. {@link uz.mpp.partnersmpp.kafkaio.ConfigChangeConsumer}).
     */
    public List<PartnerSession> sessionsForPartner(String partnerId) {
        String prefix = partnerId + ":";
        List<PartnerSession> result = new java.util.ArrayList<>();
        for (Map.Entry<String, ActiveSession> entry : sessions.entrySet()) {
            String k = entry.getKey();
            if (k.startsWith(prefix) && partnerIdOf(k).equals(partnerId)) {
                result.add(new PartnerSession(systemIdOf(k), entry.getValue()));
            }
        }
        return result;
    }
}