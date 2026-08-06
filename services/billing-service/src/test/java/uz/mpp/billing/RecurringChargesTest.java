package uz.mpp.billing;

import org.junit.jupiter.api.Test;

import java.time.YearMonth;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecurringChargesTest {

    private static final YearMonth PERIOD = YearMonth.of(2026, 8);

    @Test
    void chargesOneFeePerActiveSender() {
        List<RecurringCharges.Sender> senders = List.of(
            new RecurringCharges.Sender("CLICK", "ALPHANAME", "active"),
            new RecurringCharges.Sender("5252", "SHORT_NUMBER", "active")
        );
        List<RecurringCharges.PlannedCharge> plan = RecurringCharges.plan("click_uz", senders, 4_000_000L, null, PERIOD);

        assertEquals(2, plan.size());
        assertTrue(plan.stream().allMatch(c -> c.amount() == 4_000_000L));
    }

    @Test
    void archivedSendersAreNotCharged() {
        List<RecurringCharges.Sender> senders = List.of(
            new RecurringCharges.Sender("CLICK", "ALPHANAME", "active"),
            new RecurringCharges.Sender("OLD_NAME", "ALPHANAME", "archived")
        );
        List<RecurringCharges.PlannedCharge> plan = RecurringCharges.plan("click_uz", senders, 4_000_000L, null, PERIOD);

        assertEquals(1, plan.size());
    }

    @Test
    void nullOrZeroFeeChargesNothingForSenders() {
        List<RecurringCharges.Sender> senders = List.of(new RecurringCharges.Sender("CLICK", "ALPHANAME", "active"));

        assertEquals(0, RecurringCharges.plan("click_uz", senders, null, null, PERIOD).size());
        assertEquals(0, RecurringCharges.plan("click_uz", senders, 0L, null, PERIOD).size());
    }

    @Test
    void servicePackageAddsOneChargePerPartnerNotPerSender() {
        List<RecurringCharges.Sender> senders = List.of(
            new RecurringCharges.Sender("CLICK", "ALPHANAME", "active"),
            new RecurringCharges.Sender("5252", "SHORT_NUMBER", "active")
        );
        RecurringCharges.ServicePackage pkg = new RecurringCharges.ServicePackage(40_000, 2_000_000);
        List<RecurringCharges.PlannedCharge> plan = RecurringCharges.plan("click_uz", senders, null, pkg, PERIOD);

        assertEquals(1, plan.size(), "пакет — один charge на партнёра, не на каждый sender");
        assertEquals(2_000_000L, plan.get(0).amount());
    }

    @Test
    void noEmptyPackagePriceMeansNoCharge() {
        RecurringCharges.ServicePackage freePkg = new RecurringCharges.ServicePackage(40_000, 0);
        List<RecurringCharges.PlannedCharge> plan = RecurringCharges.plan("click_uz", List.of(), null, freePkg, PERIOD);
        assertEquals(0, plan.size());
    }

    @Test
    void chargeIdIsDeterministicAcrossCalls() {
        String id1 = RecurringCharges.chargeId("click_uz", "alphaname_fee", "CLICK", PERIOD);
        String id2 = RecurringCharges.chargeId("click_uz", "alphaname_fee", "CLICK", PERIOD);
        assertEquals(id1, id2, "тот же вход должен всегда давать тот же charge_id — от этого зависит идемпотентность повторного запуска job'а");
    }

    @Test
    void chargeIdDiffersByPeriodSoNextMonthIsANewCharge() {
        String august = RecurringCharges.chargeId("click_uz", "alphaname_fee", "CLICK", YearMonth.of(2026, 8));
        String september = RecurringCharges.chargeId("click_uz", "alphaname_fee", "CLICK", YearMonth.of(2026, 9));
        assertTrue(!august.equals(september), "разные периоды должны давать разные charge_id, иначе сентябрьский charge никогда бы не применился (ALREADY_PROCESSED из-за августовского)");
    }

    @Test
    void chargeIdDiffersBySenderSoEachSenderIsBilledIndependently() {
        String click = RecurringCharges.chargeId("click_uz", "alphaname_fee", "CLICK", PERIOD);
        String shortNum = RecurringCharges.chargeId("click_uz", "alphaname_fee", "5252", PERIOD);
        assertTrue(!click.equals(shortNum));
    }

    @Test
    void chargeIdDiffersByPartnerSoOnePartnersFeeNeverCollidesWithAnothers() {
        String a = RecurringCharges.chargeId("click_uz", "alphaname_fee", "CLICK", PERIOD);
        String b = RecurringCharges.chargeId("payme_uz", "alphaname_fee", "CLICK", PERIOD);
        assertTrue(!a.equals(b));
    }
}
