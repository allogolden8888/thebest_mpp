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

    /** Immutable identity of a currently bound session, safe to hand to background workers. */
    public record ActiveSessionRef(String partnerId, String systemId, long sessionEpoch) {
    }

    private record SessionKey(String partnerId, String systemId) {
    }

    private final Map<SessionKey, ActiveSession> sessions = new ConcurrentHashMap<>();
    private final AtomicLong epochSource = new AtomicLong();

    private static SessionKey key(String partnerId, String systemId) {
        return new SessionKey(partnerId, systemId);
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
     * Consistent-enough point-in-time snapshot for the Redis heartbeat worker.
     * A session that closes immediately after the snapshot is harmless because
     * Redis updates are guarded by {@code session_epoch}.
     */
    public List<ActiveSessionRef> activeSessionsSnapshot() {
        return sessions.entrySet().stream()
            .map(entry -> new ActiveSessionRef(
                entry.getKey().partnerId(),
                entry.getKey().systemId(),
                entry.getValue().sessionEpoch()
            ))
            .toList();
    }
}
