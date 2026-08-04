package uz.mpp.delivery;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.Map;
import uz.mpp.delivery.DeliveryService.SubmitOutcome;
import uz.mpp.platformcontracts.common.v1.Outcome;

/**
 * Закрывает CRITICAL находку кодревью (PART 2, delivery-service #1):
 * {@code queueMsgId} был свежим {@code UUID.randomUUID()} на каждый вызов
 * {@code processRecord}, включая редеставку того же {@code stage_execution_id}
 * после успешного gRPC submit, но до успешной публикации {@code stage.completed}
 * — редеставка вызывала настоящий повторный submit оператору (дубль SMS).
 *
 * Реализует ровно то, что требует {@code hld.md §21}: "Дубли предотвращаются
 * сочетанием: stable stage_execution_id; CAS в Runtime Redis; ...; запрета
 * автоматического повторного submit при UNKNOWN." — атомарный claim
 * ({@code HSETNX}) на {@code stage_execution_id} гарантирует, что реальный
 * {@code submitClient.submit(...)} вызывается не более одного раза за весь
 * TTL-жизни ключа; если claim уже занят, а результат ещё не записан (крэш
 * между submit и записью исхода), это НЕ ретраится как новый submit — по
 * правилу платформы такой случай трактуется как {@code SUBMISSION_OUTCOME_UNKNOWN},
 * тем же queue_msg_id, что был в исходной (возможно реально дошедшей до
 * оператора) попытке.
 */
public final class SubmitIdempotencyStore {

    private static final Duration TTL = Duration.ofHours(24);

    /** Итог, который нужно применить в {@code processRecord}. */
    public sealed interface ClaimResult {
        record Won(String queueMsgId) implements ClaimResult {
        }

        record AlreadyDone(SubmitOutcome outcome, String queueMsgId) implements ClaimResult {
        }

        record AmbiguousInFlight(String queueMsgId) implements ClaimResult {
        }
    }

    private final RedisClient client;

    public SubmitIdempotencyStore(String redisUrl) {
        this.client = RedisClient.create(redisUrl);
    }

    private static String key(String stageExecutionId) {
        return "dlvsubmit:" + stageExecutionId;
    }

    /**
     * {@code cas_transition}-подобный claim, но без Lua: единственная
     * операция, которой нужна атомарность — "занять этот stage_execution_id
     * под submit, если ещё не занят" — это ровно то, что делает {@code HSETNX}
     * на одном поле одной командой.
     */
    public ClaimResult claim(String stageExecutionId, String deterministicQueueMsgId) {
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> commands = connection.sync();
            String k = key(stageExecutionId);
            boolean won = commands.hsetnx(k, "queue_msg_id", deterministicQueueMsgId);
            if (won) {
                commands.expire(k, TTL);
                return new ClaimResult.Won(deterministicQueueMsgId);
            }

            Map<String, String> existing = commands.hgetall(k);
            String queueMsgId = existing.getOrDefault("queue_msg_id", deterministicQueueMsgId);
            if ("DONE".equals(existing.get("status"))) {
                Outcome outcome = Outcome.valueOf(existing.get("outcome"));
                SubmitOutcome cached = new SubmitOutcome(
                    outcome, existing.getOrDefault("reason_code", ""), existing.getOrDefault("smsc_message_id", ""));
                return new ClaimResult.AlreadyDone(cached, queueMsgId);
            }
            return new ClaimResult.AmbiguousInFlight(queueMsgId);
        }
    }

    /** Записывает исход состоявшегося (или трактованного как UNKNOWN) submit'а. */
    public void recordOutcome(String stageExecutionId, SubmitOutcome outcome) {
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> commands = connection.sync();
            String k = key(stageExecutionId);
            commands.hset(k, Map.of(
                "status", "DONE",
                "outcome", outcome.outcome().name(),
                "reason_code", outcome.reasonCode() == null ? "" : outcome.reasonCode(),
                "smsc_message_id", outcome.smscMessageId() == null ? "" : outcome.smscMessageId()));
            commands.expire(k, TTL);
        }
    }

    public void close() {
        client.shutdown();
    }
}
