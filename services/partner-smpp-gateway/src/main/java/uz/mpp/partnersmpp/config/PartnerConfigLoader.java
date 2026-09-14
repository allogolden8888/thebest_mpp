package uz.mpp.partnersmpp.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Загружает реальный партнёрский конфиг-снапшот ({@code
 * config_schemas/partner.schema.json} форма)
 * и строит {@code system_id -> credential} карту для приложений с
 * {@code auth.type == "SMPP_BIND"} — раньше (см. {@link
 * uz.mpp.partnersmpp.server.StaticAuthenticator} докстринг/README "Что НЕ
 * реализовано") этот сервис вообще не читал реальный partner-конфиг,
 * только пять env vars на один захардкоженный system_id.
 *
 * <p>Парсер используется и стартовым Redis snapshot, и immutable payload
 * каждого {@code config.changes} события. {@link #fromFile} оставлен только
 * для fixture/break-glass сценариев; production hot-reload не зависит от
 * локального файла.
 *
 * <p><b>system_id == application_id для SMPP_BIND.</b> {@code
 * partner.schema.json}'s докстринг на {@code notification_callback_url}:
 * "для auth.type=SMPP_BIND уведомление о статусе идёт через deliver_sm на
 * тот же бинд (нужен только system_id, которым здесь считается
 * application_id)" — отдельного поля {@code system_id} в схеме нет,
 * partner-notification-service (Go, {@code internal/registry/registry.go})
 * — второй независимый консьюмер этого же соглашения.
 *
 * <p>Один файл — один партнёр (top-level объект, не массив) — тот же
 * формат, что {@code config_schemas/examples/partner.valid.json} и оба
 * существующих Rust/Go консьюмера (partner_config.rs,
 * internal/config/partner.go) уже предполагают.
 */
public final class PartnerConfigLoader {

    /** {@code system_id -> (partner_id, application_id, credential_ref)}. */
    public record SmppBindCredential(String partnerId, String applicationId, String credentialRef) {
    }

    /** Parsed identity/status plus the SMPP credentials derived from one immutable partner version. */
    public record ParsedPartner(String partnerId, String status, Map<String, SmppBindCredential> credentials) {
    }

    private PartnerConfigLoader() {
    }

    public static Map<String, SmppBindCredential> fromFile(Path path) {
        try {
            return fromJson(Files.readString(path));
        } catch (IOException e) {
            throw new IllegalArgumentException("не удалось прочитать " + path, e);
        }
    }

    public static Map<String, SmppBindCredential> fromJson(String json) {
        return parseJson(json).credentials();
    }

    public static ParsedPartner parseJson(String json) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode root = mapper.readTree(json);
            String partnerId = root.path("partner_id").asText(null);
            if (partnerId == null || partnerId.isEmpty()) {
                throw new IllegalArgumentException("partner.schema.json форма обязана содержать partner_id");
            }
            String status = root.path("status").asText("");

            Map<String, SmppBindCredential> bySystemId = new LinkedHashMap<>();
            // Неактивный партнёр (status != "active") — ни одного credential
            // не публикуем, тот же принцип, что Partner::is_active() (Rust)/
            // Partner.IsActive() (Go), хотя ни один из них ещё сегодня не
            // фильтрует по этому полю на уровне снапшота — здесь фильтруем
            // явно, раз это первое место в Java, где статус реально что-то решает.
            if (!"active".equals(status)) {
                return new ParsedPartner(partnerId, status, Map.of());
            }

            for (JsonNode app : root.path("applications")) {
                JsonNode auth = app.path("auth");
                if (!"SMPP_BIND".equals(auth.path("type").asText(null))) {
                    continue;
                }
                String applicationId = app.path("application_id").asText(null);
                String credentialRef = auth.path("credential_ref").asText(null);
                if (applicationId == null || applicationId.isEmpty()
                    || credentialRef == null || credentialRef.isEmpty()) {
                    continue;
                }
                bySystemId.put(applicationId, new SmppBindCredential(partnerId, applicationId, credentialRef));
            }
            return new ParsedPartner(partnerId, status, Map.copyOf(bySystemId));
        } catch (IOException e) {
            throw new IllegalArgumentException("не удалось распарсить partner.schema.json форму", e);
        }
    }
}
