package uz.mpp.operatorsmpp.core;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

/**
 * dynamic-seeking-russell.md "Observability" — голые {@link AtomicLong}, без
 * новой зависимости (в репозитории нет ни micrometer, ни prometheus ни в
 * одном pom.xml — проверено; см. план). Рендерит тот же hand-rolled
 * {@code # HELP}/{@code # TYPE} текстовый формат, что уже используется в
 * {@link uz.mpp.operatorsmpp.health.HealthServer}/остальных сервисах серии
 * (billing-service/delivery-service {@code HealthServer.java}) — не
 * настоящий Prometheus client, просто совместимый по тексту вывод.
 */
public final class PacerMetrics {

    private final Map<PriorityTier, AtomicLong> dispatchedTotal = new EnumMap<>(PriorityTier.class);
    private final Map<PriorityTier, AtomicLong> dispatchedAtLastScrape = new EnumMap<>(PriorityTier.class);
    private final Map<String, AtomicLong> rejectedTotal = new ConcurrentHashMap<>();
    private final Map<PriorityTier, IntSupplier> queueDepthSuppliers = new EnumMap<>(PriorityTier.class);

    public PacerMetrics() {
        for (PriorityTier tier : PriorityTier.values()) {
            dispatchedTotal.put(tier, new AtomicLong());
            dispatchedAtLastScrape.put(tier, new AtomicLong());
        }
    }

    public void recordDispatched(PriorityTier tier) {
        dispatchedTotal.get(tier).incrementAndGet();
    }

    public void recordRejected(PriorityTier tier, String reason) {
        rejectedTotal.computeIfAbsent(rejectedKey(tier, reason), k -> new AtomicLong()).incrementAndGet();
    }

    /**
     * {@code queue_depth{tier}} читается из реальной очереди В МОМЕНТ scrape
     * (не кэшированный снапшот) — Main.java регистрирует
     * {@code queue::size} каждого из 3 {@code ArrayBlockingQueue} сюда один
     * раз при старте.
     */
    public void bindQueueDepth(PriorityTier tier, IntSupplier liveDepth) {
        queueDepthSuppliers.put(tier, liveDepth);
    }

    long dispatchedTotalForTest(PriorityTier tier) {
        return dispatchedTotal.get(tier).get();
    }

    long rejectedTotalForTest(PriorityTier tier, String reason) {
        AtomicLong counter = rejectedTotal.get(rejectedKey(tier, reason));
        return counter == null ? 0 : counter.get();
    }

    private static String rejectedKey(PriorityTier tier, String reason) {
        return tier.name() + "|" + reason;
    }

    /**
     * Полный {@code /metrics} текст пейсера (без {@code pending_response_count} —
     * тот собирается отдельно в HealthServer напрямую из OperatorSmppClient,
     * см. план: "wire the already-existing but currently unexposed
     * OperatorSmppClient.pendingResponseCount() into the same output").
     */
    public String renderPrometheusText() {
        StringBuilder sb = new StringBuilder();

        sb.append("# HELP operator_smpp_pacer_dispatched_total Сколько submit допущено к отправке пейсером, по tier'ам\n");
        sb.append("# TYPE operator_smpp_pacer_dispatched_total counter\n");
        for (PriorityTier tier : PriorityTier.values()) {
            sb.append("operator_smpp_pacer_dispatched_total{tier=\"").append(tierLabel(tier)).append("\"} ")
                .append(dispatchedTotal.get(tier).get()).append('\n');
        }

        sb.append("# HELP operator_smpp_pacer_rejected_total Сколько submit отклонено пейсером, по tier'ам и причине\n");
        sb.append("# TYPE operator_smpp_pacer_rejected_total counter\n");
        rejectedTotal.forEach((key, counter) -> {
            int sep = key.indexOf('|');
            String tierLabel = key.substring(0, sep).toLowerCase(java.util.Locale.ROOT);
            String reason = key.substring(sep + 1);
            sb.append("operator_smpp_pacer_rejected_total{tier=\"").append(tierLabel)
                .append("\",reason=\"").append(reason).append("\"} ")
                .append(counter.get()).append('\n');
        });

        sb.append("# HELP operator_smpp_pacer_queue_depth Текущая глубина очереди пейсера в момент scrape, по tier'ам\n");
        sb.append("# TYPE operator_smpp_pacer_queue_depth gauge\n");
        for (PriorityTier tier : PriorityTier.values()) {
            IntSupplier supplier = queueDepthSuppliers.get(tier);
            int depth = supplier == null ? 0 : supplier.getAsInt();
            sb.append("operator_smpp_pacer_queue_depth{tier=\"").append(tierLabel(tier)).append("\"} ").append(depth).append('\n');
        }

        // Буквально "дельта с прошлого scrape" (план), не нормализованная
        // rate() по времени — здесь нет Prometheus, который бы это сделал
        // сам, поэтому число само по себе имеет смысл "TPS" только при
        // ~1с интервале между scrape (verification в плане: "shows real,
        // moving numbers during a load test", не точная rate-метрика).
        sb.append("# HELP operator_smpp_pacer_achieved_tps Дельта dispatched_total с прошлого /metrics scrape, по tier'ам\n");
        sb.append("# TYPE operator_smpp_pacer_achieved_tps gauge\n");
        for (PriorityTier tier : PriorityTier.values()) {
            long current = dispatchedTotal.get(tier).get();
            long previous = dispatchedAtLastScrape.get(tier).getAndSet(current);
            sb.append("operator_smpp_pacer_achieved_tps{tier=\"").append(tierLabel(tier)).append("\"} ").append(current - previous).append('\n');
        }

        return sb.toString();
    }

    private static String tierLabel(PriorityTier tier) {
        return tier.name().toLowerCase(java.util.Locale.ROOT);
    }
}
