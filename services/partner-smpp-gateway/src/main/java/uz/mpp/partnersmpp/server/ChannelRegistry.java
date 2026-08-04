package uz.mpp.partnersmpp.server;

import io.netty.channel.Channel;

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

    private final Map<String, ActiveSession> sessions = new ConcurrentHashMap<>();
    private final AtomicLong epochSource = new AtomicLong();

    private static String key(String partnerId, String systemId) {
        return partnerId + ":" + systemId;
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
}