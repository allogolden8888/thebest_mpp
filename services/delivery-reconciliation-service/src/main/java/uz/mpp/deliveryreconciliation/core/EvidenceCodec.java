package uz.mpp.deliveryreconciliation.core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * CODE_REVIEW.md #11 — сериализация {@link Evidence} для колонки
 * {@code reconciliation_cases.evidence} (JSONB): {@code collect_evidence}
 * копится по мере поступления {@code operator.submit.accepted}/
 * {@code delivery.status} в разных Kafka-consumer потоках, между poll'ами
 * персистится через {@code ReconciliationStore.persistEvidence}, и должна
 * быть прочитана обратно перед {@code resolve_outcome} в {@code sweepDeadlines}
 * (до этой находки читалось {@link Evidence#empty()} — сам предмет #11).
 *
 * <p>Хендролленый JSON, без внешней зависимости (Jackson/Gson не подключены
 * в pom.xml этого сервиса, тот же выбор, что остальные Java-сервисы этой
 * сессии — не тянуть библиотеку ради одной плоской записи из 3 полей).
 * Формат согласован с уже существующим тестом
 * {@code ReconciliationStoreTest.persistEvidenceUpdatesEvidenceColumn}
 * (сырой {@code {"submit_accepted":true}}) — decode терпим к отсутствующим
 * полям (значение по умолчанию), не требует полной схемы.
 */
public final class EvidenceCodec {

    private static final Pattern SUBMIT_ACCEPTED = Pattern.compile("\"submit_accepted\"\\s*:\\s*(true|false)");
    private static final Pattern DELIVERY_STATUS = Pattern.compile("\"delivery_status\"\\s*:\\s*\"([A-Z_]+)\"");
    private static final Pattern QUERY_SM = Pattern.compile("\"query_sm\"\\s*:\\s*\"([A-Z_]+)\"");

    private EvidenceCodec() {
    }

    public static String encode(Evidence evidence) {
        return "{\"submit_accepted\":" + evidence.submitAcceptedObserved()
            + ",\"delivery_status\":\"" + evidence.deliveryStatusObserved() + "\""
            + ",\"query_sm\":\"" + evidence.querySmObserved() + "\"}";
    }

    /** Терпим к {@code null}/{@code "{}"}/частичному JSON — недостающие поля берут значения {@link Evidence#empty()}. */
    public static Evidence decode(String json) {
        if (json == null || json.isBlank()) {
            return Evidence.empty();
        }
        boolean submitAccepted = matchGroup(SUBMIT_ACCEPTED, json).map(Boolean::parseBoolean).orElse(false);
        Evidence.DeliveryOutcome deliveryStatus = matchGroup(DELIVERY_STATUS, json)
            .map(v -> parseEnum(Evidence.DeliveryOutcome.class, v, Evidence.DeliveryOutcome.NONE))
            .orElse(Evidence.DeliveryOutcome.NONE);
        Evidence.QuerySmOutcome querySm = matchGroup(QUERY_SM, json)
            .map(v -> parseEnum(Evidence.QuerySmOutcome.class, v, Evidence.QuerySmOutcome.NOT_CALLED))
            .orElse(Evidence.QuerySmOutcome.NOT_CALLED);
        return new Evidence(submitAccepted, deliveryStatus, querySm);
    }

    private static java.util.Optional<String> matchGroup(Pattern pattern, String json) {
        Matcher m = pattern.matcher(json);
        return m.find() ? java.util.Optional.of(m.group(1)) : java.util.Optional.empty();
    }

    private static <E extends Enum<E>> E parseEnum(Class<E> type, String value, E fallback) {
        try {
            return Enum.valueOf(type, value);
        } catch (IllegalArgumentException e) {
            return fallback; // неизвестное/повреждённое значение — деградируем к дефолту, не бросаем
        }
    }
}
