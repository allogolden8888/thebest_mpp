package uz.mpp.billing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * {@code resolve_tariff} (service_internal_methods.md §1.6) — чистый lookup
 * {@code price_per_segment[category] × segment_count}, без решения о
 * продолжении pipeline. Форма данных — та же, что {@code config_schemas/billing_tariff.schema.json}
 * / {@code data_infrastructure_spec.md} §1.6a: {@code BLOCKED} обязателен
 * (отклонённое Policy сообщение всё равно тарифицируется).
 */
public final class TariffResolver {

    private final Map<String, Long> pricePerSegment;
    private final String currencyCode;
    private final Long alphanameMonthlyFee;
    private final RecurringCharges.ServicePackage servicePackage;

    private TariffResolver(Map<String, Long> pricePerSegment, String currencyCode, Long alphanameMonthlyFee, RecurringCharges.ServicePackage servicePackage) {
        this.pricePerSegment = pricePerSegment;
        this.currencyCode = currencyCode;
        this.alphanameMonthlyFee = alphanameMonthlyFee;
        this.servicePackage = servicePackage;
    }

    public static TariffResolver fromConfigSchemaJson(String json) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode root = mapper.readTree(json);
            Map<String, Long> prices = new HashMap<>();
            root.get("price_per_segment").fields().forEachRemaining(e -> prices.put(e.getKey(), e.getValue().asLong()));
            if (!prices.containsKey("BLOCKED")) {
                throw new IllegalArgumentException("price_per_segment обязан содержать BLOCKED (data_infrastructure_spec.md §1.6a)");
            }

            // recurring_charges — development_plan.md 5.4, опционально
            // (billing_tariff.schema.json не требует эту секцию — старые
            // конфиги без неё остаются валидными).
            Long alphanameMonthlyFee = null;
            RecurringCharges.ServicePackage servicePackage = null;
            JsonNode recurring = root.get("recurring_charges");
            if (recurring != null) {
                JsonNode feeNode = recurring.get("alphaname_monthly_fee");
                if (feeNode != null) {
                    alphanameMonthlyFee = feeNode.asLong();
                }
                JsonNode pkgNode = recurring.get("service_sms_package");
                if (pkgNode != null) {
                    servicePackage = new RecurringCharges.ServicePackage(pkgNode.get("segments").asLong(), pkgNode.get("price").asLong());
                }
            }

            return new TariffResolver(prices, root.get("currency").asText(), alphanameMonthlyFee, servicePackage);
        } catch (IOException e) {
            throw new IllegalArgumentException("не удалось распарсить billing_tariff.schema.json форму", e);
        }
    }

    public Long alphanameMonthlyFee() {
        return alphanameMonthlyFee;
    }

    public RecurringCharges.ServicePackage servicePackage() {
        return servicePackage;
    }

    public static TariffResolver fromFile(Path path) {
        try {
            return fromConfigSchemaJson(Files.readString(path));
        } catch (IOException e) {
            throw new IllegalArgumentException("не удалось прочитать " + path, e);
        }
    }

    public record Tariff(long amountMinorUnits, String currencyCode) {
    }

    public Tariff resolve(String category, int segmentCount) {
        Long perSegment = pricePerSegment.get(category);
        if (perSegment == null) {
            // Не должно происходить для реального трафика (category всегда одна из
            // known + BLOCKED/UNTEMPLATED, service_internal_methods.md §1.5), но
            // стадия не должна падать из-за отсутствующей записи в тарифе — см.
            // комментарий к BLOCKED в data_infrastructure_spec.md §1.6a.
            throw new IllegalArgumentException("нет тарифа для категории " + category);
        }
        return new Tariff(perSegment * segmentCount, currencyCode);
    }
}
