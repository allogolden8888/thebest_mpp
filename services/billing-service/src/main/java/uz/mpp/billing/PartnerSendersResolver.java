package uz.mpp.billing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Читает {@code senders[]} и {@code partner_id} из файла, форма которого
 * — {@code config_schemas/partner.schema.json} — тот же приём, что
 * {@link TariffResolver#fromFile}: статический файл, путь из env var,
 * читается один раз при старте (services_specifictaion.md §1.6
 * не специфицирует live-обновление partner-конфига для Billing; полный
 * config.changes consumer — отдельная задача, не в этом срезе, см.
 * README).
 */
public final class PartnerSendersResolver {

    private PartnerSendersResolver() {
    }

    public record PartnerSenders(String partnerId, List<RecurringCharges.Sender> senders) {
    }

    public static PartnerSenders fromFile(Path path) {
        try {
            return fromJson(Files.readString(path));
        } catch (IOException e) {
            throw new IllegalArgumentException("не удалось прочитать " + path, e);
        }
    }

    public static PartnerSenders fromJson(String json) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode root = mapper.readTree(json);
            String partnerId = root.path("partner_id").asText(null);
            if (partnerId == null || partnerId.isEmpty()) {
                throw new IllegalArgumentException("partner.schema.json форма обязана содержать partner_id");
            }
            List<RecurringCharges.Sender> senders = new ArrayList<>();
            for (JsonNode s : root.path("senders")) {
                senders.add(new RecurringCharges.Sender(
                    s.path("sender_id").asText(),
                    s.path("type").asText(),
                    s.path("status").asText()));
            }
            return new PartnerSenders(partnerId, senders);
        } catch (IOException e) {
            throw new IllegalArgumentException("не удалось распарсить partner.schema.json форму", e);
        }
    }
}
