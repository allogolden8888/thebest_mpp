package uz.mpp.billing;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TariffResolverTest {

    private static TariffResolver realTariff() {
        Path path = Path.of(System.getProperty("user.dir"), "..", "..", "config_schemas", "examples", "billing_tariff.valid.json");
        return TariffResolver.fromFile(path);
    }

    @Test
    void transactionCategoryPricedCorrectly() {
        // config_schemas/examples/billing_tariff.valid.json: реальный тариф
        // партнёра (2026-08), не placeholder — TRANSACTION=94.
        TariffResolver.Tariff tariff = realTariff().resolve("TRANSACTION", 2);
        assertEquals(188, tariff.amountMinorUnits());
        assertEquals("UZS", tariff.currencyCode());
    }

    @Test
    void blockedCategoryHasNonZeroTariff() {
        // Отклонённое Policy сообщение всё равно тарифицируется — BLOCKED=94 в реальном тарифе.
        TariffResolver.Tariff tariff = realTariff().resolve("BLOCKED", 1);
        assertEquals(94, tariff.amountMinorUnits());
    }

    @Test
    void untemplatedCategoryIsMoreExpensiveThanKnownTemplates() {
        TariffResolver.Tariff untemplated = realTariff().resolve("UNTEMPLATED", 1);
        TariffResolver.Tariff service = realTariff().resolve("SERVICE", 1);
        assertEquals(3500, untemplated.amountMinorUnits());
        assertEquals(94, service.amountMinorUnits());
    }

    @Test
    void unknownCategoryThrowsRatherThanSilentlyChargingZero() {
        assertThrows(IllegalArgumentException.class, () -> realTariff().resolve("NOT_A_REAL_CATEGORY", 1));
    }

    @Test
    void missingBlockedKeyRejectedAtLoadTime() {
        String malformedJson = """
            {"currency": "UZS", "price_per_segment": {"SERVICE": 100}, "default_category": "UNTEMPLATED"}
            """;
        assertThrows(IllegalArgumentException.class, () -> TariffResolver.fromConfigSchemaJson(malformedJson));
    }
}
