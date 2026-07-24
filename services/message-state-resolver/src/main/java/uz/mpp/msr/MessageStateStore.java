package uz.mpp.msr;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code load_current_state} — HLD §10.1: "Authoritative state хранится в
 * {@code message-state.changelog}. Embedded KV является только локальной
 * материализованной копией." Здесь эта локальная копия — in-memory
 * {@link ConcurrentHashMap}, не RocksDB — тот же класс упрощения, что
 * {@code ExecutionState} в pipeline-engine (`Arc<Mutex<HashMap>>` вместо
 * Redis CAS): реальная транзакционная гарантия (changelog-запись +
 * message.lifecycle + offset commit одной Kafka-транзакцией) реализована
 * по-настоящему в {@code KafkaIo} — упрощена только ЛОКАЛЬНАЯ проекция,
 * не сама гарантия. **Важное ограничение, не мелочь**: как и
 * pipeline-engine, при рестарте пода состояние теряется (нет
 * restore-from-changelog в этом срезе) — см. README.
 */
public final class MessageStateStore {

    private final Map<String, LifecycleState> states = new ConcurrentHashMap<>();

    public LifecycleState get(String messageId) {
        return states.getOrDefault(messageId, LifecycleState.fresh());
    }

    public void put(String messageId, LifecycleState state) {
        states.put(messageId, state);
    }
}
