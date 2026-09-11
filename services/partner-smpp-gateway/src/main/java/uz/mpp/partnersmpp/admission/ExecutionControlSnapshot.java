package uz.mpp.partnersmpp.admission;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import uz.mpp.platformcontracts.common.v1.ExecutionControlScope;
import uz.mpp.platformcontracts.common.v1.ExecutionControlState;
import uz.mpp.platformcontracts.events.v1.ExecutionControlRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Atomic local view of the GLOBAL/PARTNER part of {@code execution.control}.
 *
 * <p>The empty/default state is deliberately fail-closed. A replay becomes
 * visible atomically only after the consumer has reached the captured end of
 * every topic partition. Live updates are copy-on-write, so Netty event-loop
 * threads never wait on Kafka or on a lock.</p>
 */
public final class ExecutionControlSnapshot {

    private enum KeyType { GLOBAL, PARTNER }

    private record ControlKey(KeyType type, String scopeId) {
        static ControlKey global() {
            return new ControlKey(KeyType.GLOBAL, "");
        }

        static ControlKey partner(String partnerId) {
            return new ControlKey(KeyType.PARTNER, partnerId);
        }
    }

    private record ControlValue(
        ExecutionControlState state,
        double admissionRate,
        Instant expiresAt
    ) {
        boolean expiredAt(Instant now) {
            return expiresAt != null && !expiresAt.isAfter(now);
        }
    }

    private record State(boolean bootstrapped, Map<ControlKey, ControlValue> records) {
    }

    /** Mutable only while a Kafka replay is private to its consumer thread. */
    static final class Replay {
        private final Map<ControlKey, ControlValue> records = new HashMap<>();

        void apply(byte[] kafkaKey, byte[] payload) {
            applyTo(records, kafkaKey, payload);
        }
    }

    private volatile State state = new State(false, Map.of());

    public boolean isReady() {
        State current = state;
        ControlValue global = current.records().get(ControlKey.global());
        return current.bootstrapped() && global != null && !global.expiredAt(Instant.now());
    }

    /** Called once a full replay of every currently known partition completes. */
    synchronized void install(Replay replay) {
        state = new State(true, Map.copyOf(replay.records));
    }

    /** Applies a post-bootstrap Kafka record without mutating readers' maps. */
    synchronized void applyLive(byte[] kafkaKey, byte[] payload) {
        Map<ControlKey, ControlValue> updated = new HashMap<>(state.records());
        applyTo(updated, kafkaKey, payload);
        state = new State(state.bootstrapped(), Map.copyOf(updated));
    }

    Optional<Double> effectiveRate(String partnerId) {
        return effectiveRateAt(partnerId, Instant.now());
    }

    Optional<Double> effectiveRateAt(String partnerId, Instant now) {
        State current = state;
        if (!current.bootstrapped()) {
            return Optional.empty();
        }

        ControlValue global = current.records().get(ControlKey.global());
        if (global == null || global.expiredAt(now)
            || global.state() == ExecutionControlState.EXECUTION_CONTROL_STATE_PAUSED) {
            return Optional.empty();
        }

        double rate = global.admissionRate();
        ControlValue partner = current.records().get(ControlKey.partner(partnerId));
        if (partner != null && !partner.expiredAt(now)) {
            if (partner.state() == ExecutionControlState.EXECUTION_CONTROL_STATE_PAUSED) {
                return Optional.empty();
            }
            rate = Math.min(rate, partner.admissionRate());
        }
        return Optional.of(rate);
    }

    private static void applyTo(Map<ControlKey, ControlValue> records, byte[] kafkaKey, byte[] payload) {
        if (payload == null) {
            Optional<ControlKey> key = parseKey(requireKafkaKey(kafkaKey, "tombstone"));
            key.ifPresent(records::remove);
            return;
        }

        ExecutionControlRecord record;
        try {
            record = ExecutionControlRecord.parseFrom(payload);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("cannot decode ExecutionControlRecord", e);
        }

        ExecutionControlScope scope = ExecutionControlScope.forNumber(record.getScopeValue());
        if (scope == null || scope == ExecutionControlScope.EXECUTION_CONTROL_SCOPE_UNSPECIFIED) {
            throw new IllegalArgumentException("execution.control contains unknown/unspecified scope=" + record.getScopeValue());
        }
        ExecutionControlState controlState = ExecutionControlState.forNumber(record.getStateValue());
        if (controlState == null || controlState == ExecutionControlState.EXECUTION_CONTROL_STATE_UNSPECIFIED) {
            throw new IllegalArgumentException("execution.control contains unknown/unspecified state=" + record.getStateValue());
        }
        if (!Double.isFinite(record.getAdmissionRate())
            || record.getAdmissionRate() < 0.0 || record.getAdmissionRate() > 1.0) {
            throw new IllegalArgumentException("execution.control admission_rate must be finite and within [0,1]");
        }

        Optional<ControlKey> controlKey = keyFor(scope, record.getScopeId());
        if (controlKey.isEmpty()) {
            // STAGE/PARTNER_STAGE/OPERATOR_ROUTE are valid, but ingress cannot
            // apply them before routing/stage selection.
            return;
        }
        ControlKey expected = controlKey.orElseThrow();
        Optional<ControlKey> actual = parseKey(requireKafkaKey(kafkaKey, "record"));
        if (actual.isEmpty() || !actual.orElseThrow().equals(expected)) {
            throw new IllegalArgumentException("execution.control Kafka key does not match protobuf scope/scope_id");
        }

        Instant expiresAt = null;
        if (record.hasExpiresAt()) {
            Timestamp timestamp = record.getExpiresAt();
            if (timestamp.getNanos() < 0 || timestamp.getNanos() >= 1_000_000_000) {
                throw new IllegalArgumentException("execution.control expires_at nanos is invalid");
            }
            expiresAt = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
        }
        records.put(expected, new ControlValue(controlState, record.getAdmissionRate(), expiresAt));
    }

    private static byte[] requireKafkaKey(byte[] key, String kind) {
        if (key == null) {
            throw new IllegalArgumentException("execution.control " + kind + " has no Kafka key");
        }
        return key;
    }

    private static Optional<ControlKey> parseKey(byte[] raw) {
        String text = new String(raw, StandardCharsets.UTF_8);
        int delimiter = text.indexOf(':');
        if (delimiter < 0) {
            throw new IllegalArgumentException("execution.control key must have SCOPE:scope_id form");
        }
        String scopeName = text.substring(0, delimiter);
        String scopeId = text.substring(delimiter + 1);
        ExecutionControlScope scope;
        try {
            scope = ExecutionControlScope.valueOf(scopeName);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("execution.control key contains unknown scope " + scopeName, e);
        }
        return keyFor(scope, scopeId);
    }

    private static Optional<ControlKey> keyFor(ExecutionControlScope scope, String scopeId) {
        return switch (scope) {
            case EXECUTION_CONTROL_SCOPE_GLOBAL -> {
                if (!scopeId.isEmpty()) {
                    throw new IllegalArgumentException("GLOBAL execution.control must have empty scope_id");
                }
                yield Optional.of(ControlKey.global());
            }
            case EXECUTION_CONTROL_SCOPE_PARTNER -> {
                if (scopeId.isEmpty()) {
                    throw new IllegalArgumentException("PARTNER execution.control must have non-empty scope_id");
                }
                yield Optional.of(ControlKey.partner(scopeId));
            }
            case EXECUTION_CONTROL_SCOPE_STAGE,
                 EXECUTION_CONTROL_SCOPE_PARTNER_STAGE,
                 EXECUTION_CONTROL_SCOPE_OPERATOR_ROUTE -> Optional.empty();
            case EXECUTION_CONTROL_SCOPE_UNSPECIFIED, UNRECOGNIZED ->
                throw new IllegalArgumentException("execution.control contains unspecified scope");
        };
    }
}
