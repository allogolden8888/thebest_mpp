package uz.mpp.partnersmpp.config;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link PartnerConfigLoader} — реальный файл {@code
 * config_schemas/examples/partner.valid.json} на этот момент содержит
 * только {@code auth.type=API_KEY} приложения (проверено: {@link
 * #realFixtureHasNoSmppBindApplicationsYet()} — честно фиксирует этот
 * пробел, а не молчит о нём), поэтому "находит реальные SMPP_BIND
 * приложения" проверяется на JSON, повторяющем ТУ ЖЕ форму схемы
 * ({@code config_schemas/partner.schema.json}) с добавленным SMPP_BIND
 * приложением — не модификация общего файла {@code
 * config_schemas/examples/partner.valid.json} (им параллельно владеет
 * другой агент/сервис, вне периметра этой задачи), просто in-test строка.
 */
class PartnerConfigLoaderTest {

    private static final Path REAL_FIXTURE =
        Path.of("../../config_schemas/examples/partner.valid.json");

    @Test
    void realFixtureHasNoSmppBindApplicationsYet() {
        Map<String, PartnerConfigLoader.SmppBindCredential> creds = PartnerConfigLoader.fromFile(REAL_FIXTURE);
        // Честно документируем текущее состояние общего fixture-файла, не
        // изобретаем SMPP_BIND-запись в нём самостоятельно (вне периметра
        // этой задачи, владеет другой параллельный агент/партнёр-конфиг).
        assertTrue(creds.isEmpty(),
            "config_schemas/examples/partner.valid.json пока не содержит SMPP_BIND приложений; "
                + "если это изменилось — обновите README partner-smpp-gateway и этот тест");
    }

    @Test
    void realFixtureParsesWithoutErrorAndHasApiKeyApplications() {
        // Убеждаемся, что мы реально читаем/парсим настоящий файл (не
        // заглушку) — partner_id и общая форма совпадают с тем, что
        // billing-service/partner-rest-receiver уже читают из этого файла.
        String json = readReal();
        assertTrue(json.contains("\"click_uz\""));
        assertTrue(json.contains("\"API_KEY\""));
    }

    @Test
    void schemaShapedJsonWithSmppBindApplicationIsExtracted() {
        String json = """
            {
              "partner_id": "click_uz",
              "version": 5,
              "status": "active",
              "senders": [],
              "applications": [
                {
                  "application_id": "click_uz_main",
                  "display_name": "Click main billing notifications",
                  "auth": { "type": "API_KEY", "credential_ref": "vault://partners/click_uz/main/api_key" },
                  "ip_allowlist": ["185.65.212.0/24"],
                  "rate_limit_tps": 300,
                  "allowed_channels": ["SMS"],
                  "notification_callback_url": "https://api.click.uz/mpp/notifications"
                },
                {
                  "application_id": "click_uz_smpp",
                  "display_name": "Click SMPP bind",
                  "auth": { "type": "SMPP_BIND", "credential_ref": "vault://partners/click_uz/smpp/bind_password" },
                  "ip_allowlist": ["185.65.212.0/24"],
                  "rate_limit_tps": 300,
                  "allowed_channels": ["SMS"]
                }
              ]
            }
            """;

        Map<String, PartnerConfigLoader.SmppBindCredential> creds = PartnerConfigLoader.fromJson(json);

        assertEquals(1, creds.size());
        PartnerConfigLoader.SmppBindCredential cred = creds.get("click_uz_smpp");
        assertNotNull(cred, "system_id == application_id для SMPP_BIND");
        assertEquals("click_uz", cred.partnerId());
        assertEquals("click_uz_smpp", cred.applicationId());
        assertEquals("vault://partners/click_uz/smpp/bind_password", cred.credentialRef());
        // API_KEY приложение не должно попасть в SMPP-карту.
        assertNull(creds.get("click_uz_main"));
    }

    @Test
    void inactivePartnerYieldsNoCredentials() {
        String json = """
            {
              "partner_id": "click_uz",
              "version": 1,
              "status": "suspended",
              "applications": [
                { "application_id": "x", "display_name": "x",
                  "auth": { "type": "SMPP_BIND", "credential_ref": "vault://partners/click_uz/x/pw" },
                  "ip_allowlist": [], "rate_limit_tps": 10, "allowed_channels": ["SMS"] }
              ]
            }
            """;
        assertTrue(PartnerConfigLoader.fromJson(json).isEmpty());
    }

    @Test
    void missingPartnerIdThrows() {
        assertThrows(IllegalArgumentException.class, () -> PartnerConfigLoader.fromJson("{\"applications\": []}"));
    }

    private static String readReal() {
        try {
            return java.nio.file.Files.readString(REAL_FIXTURE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
