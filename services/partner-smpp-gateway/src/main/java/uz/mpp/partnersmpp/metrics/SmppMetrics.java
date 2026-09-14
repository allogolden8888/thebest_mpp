package uz.mpp.partnersmpp.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.util.concurrent.TimeUnit;

/**
 * BACKOFFICE_ROADMAP.md P1 "Observability" — заменяет прежнюю /metrics-
 * заглушку ("partner_smpp_gateway_up 1", см. health.HealthServer) реальными
 * счётчиком/гистограммой хот-пути ({@code handleSubmitSm},
 * см. server.SmppServerHandler). Micrometer — единственный metrics-фреймворк,
 * прямо названный для этого сервиса в задаче наблюдаемости (ни один
 * Java-сервис в репозитории раньше не тянул metrics-библиотеку вообще, так
 * что прецедента "уже используемого" выбора нет — Micrometer + Prometheus
 * registry — промышленный стандарт для JVM).
 *
 * <p>Один статический {@link PrometheusMeterRegistry} на процесс — по одному
 * {@link uz.mpp.partnersmpp.server.SmppServerHandler} на TCP-соединение
 * (см. PartnerSmppServer), поэтому счётчики не могут жить на самом handler'е:
 * они обязаны быть общими на все одновременные SMPP-сессии этого пода, точно
 * так же, как /metrics — общий HTTP-эндпоинт на процесс, не на соединение.
 */
public final class SmppMetrics {

    public static final PrometheusMeterRegistry REGISTRY = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    private SmppMetrics() {
    }

    /**
     * outcome — низкая кардинальность, закрытый набор веток
     * {@code handleSubmitSm} (см. SmppServerHandler): {@code ok},
     * {@code invalid_bind_state}, {@code invalid_message}, {@code throttled},
     * {@code publish_failed}.
     */
    public static void recordSubmit(String outcome, long elapsedNanos) {
        Counter.builder("partner_smpp_gateway_submit_sm_total")
            .description("Общее число submit_sm PDU, обработанных handleSubmitSm, по исходу.")
            .tag("outcome", outcome)
            .register(REGISTRY)
            .increment();
        Timer.builder("partner_smpp_gateway_submit_sm_duration_seconds")
            .description("Латентность handleSubmitSm целиком, включая ожидание Kafka-ack перед submit_sm_resp.")
            .register(REGISTRY)
            .record(elapsedNanos, TimeUnit.NANOSECONDS);
    }

    public static String scrape() {
        return REGISTRY.scrape();
    }
}
