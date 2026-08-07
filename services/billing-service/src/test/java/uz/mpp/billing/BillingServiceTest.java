package uz.mpp.billing;

import org.junit.jupiter.api.Test;
import uz.mpp.billing.BillingAccountState.Account;
import uz.mpp.platformcontracts.common.v1.BillingExtension;
import uz.mpp.platformcontracts.common.v1.Outcome;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;
import uz.mpp.platformcontracts.common.v1.StageName;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BillingServiceTest {

    private static TariffResolver realTariff() {
        Path path = Path.of(System.getProperty("user.dir"), "..", "..", "config_schemas", "examples", "billing_tariff.valid.json");
        return TariffResolver.fromFile(path);
    }

    private static StageExecuteCommand command(String stageExecutionId, String category, int segmentCount) {
        return StageExecuteCommand.newBuilder()
            .setEventId("e1")
            .setMessageId("m1")
            .setStageName(StageName.STAGE_NAME_BILLING)
            .setStageExecutionId(stageExecutionId)
            .setAttempt(1)
            .setTraceparent("tp1")
            .setBilling(BillingExtension.newBuilder()
                .setResolvedOperatorId("beeline")
                .setSegmentCount(segmentCount)
                .setCategory(category)
                .build())
            .build();
    }

    @Test
    void appliedChargeProducesSucceededWithCorrectAmount() {
        BillingService service = new BillingService(realTariff());
        Account account = Account.fresh(100_000);

        BillingService.Result result = service.handleBillingExecute(command("se1", "TRANSACTION", 2), account, account.epoch());

        StageCompletedEvent event = result.event();
        assertEquals(Outcome.OUTCOME_SUCCEEDED, event.getOutcome());
        assertEquals(188, event.getBilling().getAmount().getMinorUnits()); // TRANSACTION=94 * 2 segments
        assertEquals("TRANSACTION", event.getBilling().getCategory());
        assertEquals("se1", event.getBilling().getChargeId());
        assertEquals(99_812, result.updatedAccount().balance());
    }

    @Test
    void blockedCategoryStillChargesNonZero() {
        // Policy REJECTED сообщение всё равно идёт в Billing с category=BLOCKED
        // (hld.md §5.3.1) — Billing не знает и не решает, что REJECTED, просто тарифицирует.
        BillingService service = new BillingService(realTariff());
        Account account = Account.fresh(100_000);

        BillingService.Result result = service.handleBillingExecute(command("se-blocked", "BLOCKED", 1), account, account.epoch());

        assertEquals(Outcome.OUTCOME_SUCCEEDED, result.event().getOutcome(), "тарификация BLOCKED — это SUCCEEDED со стороны Billing, не REJECTED");
        assertEquals(94, result.event().getBilling().getAmount().getMinorUnits());
    }

    @Test
    void duplicateStageExecutionIdDoesNotDoubleCharge() {
        BillingService service = new BillingService(realTariff());
        Account account = Account.fresh(100_000);

        BillingService.Result first = service.handleBillingExecute(command("se-dup", "SERVICE", 1), account, account.epoch());
        BillingService.Result second = service.handleBillingExecute(command("se-dup", "SERVICE", 1), first.updatedAccount(), first.updatedAccount().epoch());

        assertEquals(Outcome.OUTCOME_SUCCEEDED, second.event().getOutcome(), "повторная доставка того же stage_execution_id — идемпотентный успех, не ошибка");
        assertEquals(first.updatedAccount().balance(), second.updatedAccount().balance(), "повторный retry не должен списывать дважды");
    }

    @Test
    void frozenAccountRejectedAsRetryable() {
        BillingService service = new BillingService(realTariff());
        Account frozen = BillingAccountState.freeze(Account.fresh(100_000));

        BillingService.Result result = service.handleBillingExecute(command("se-frozen", "SERVICE", 1), frozen, frozen.epoch());

        assertEquals(Outcome.OUTCOME_REJECTED, result.event().getOutcome());
        assertEquals("ACCOUNT_FROZEN", result.event().getReasonCode());
        assertTrue(result.event().getRetryable(), "заморозка временная — исполнение должно повториться после unfreeze");
        assertFalse(result.event().hasBilling(), "нет успешного списания — BillingResult не заполнен");
    }

    @Test
    void staleEpochRejectedAsRetryableNotSilentlyApplied() {
        // Воспроизводит ровно тот race, что BillingAccountStateTest#inFlightChargeRejectedByStaleEpochRace
        // доказывает на уровне state machine — здесь то же самое доказано на уровне
        // оркестрации: expectedEpoch, зафиксированный на момент диспетчеризации команды
        // (0), устарел к моменту фактического применения charge (реальный epoch — 1,
        // счёт успели заморозить/разморозить между диспетчеризацией и обработкой).
        BillingService service = new BillingService(realTariff());
        Account account = Account.fresh(100_000);
        long expectedEpochAtDispatch = account.epoch(); // 0 — то, что "видела" команда при диспетчеризации

        Account driftedAccount = BillingAccountState.unfreeze(BillingAccountState.freeze(account), 100_000, 1); // epoch теперь 2

        BillingService.Result result = service.handleBillingExecute(
            command("se-stale", "SERVICE", 1), driftedAccount, expectedEpochAtDispatch);

        assertEquals(Outcome.OUTCOME_REJECTED, result.event().getOutcome());
        assertEquals("STALE_EPOCH", result.event().getReasonCode());
        assertTrue(result.event().getRetryable());
        assertFalse(result.event().hasBilling(), "устаревшая попытка не должна была списать деньги");
        assertEquals(100_000, result.updatedAccount().balance(), "баланс не изменился — charge реально не применился");
    }

    @Test
    void negativeSegmentCountRejectedBeforeArithmetic() {
        // Кодревью: непровалидированный отрицательный segment_count инвертирует
        // charge в credit (perSegment * segmentCount с отрицательным множителем),
        // и applyCharge докладывает это как обычный SUCCEEDED. Обязано быть
        // отклонено до того, как дойдёт до BillingAccountState.applyCharge.
        BillingService service = new BillingService(realTariff());
        Account account = Account.fresh(100_000);
        StageExecuteCommand command = command("se-negative", "SERVICE", -3);

        assertThrows(IllegalArgumentException.class, () -> service.handleBillingExecute(command, account, account.epoch()));
    }

    @Test
    void zeroSegmentCountRejected() {
        BillingService service = new BillingService(realTariff());
        Account account = Account.fresh(100_000);
        StageExecuteCommand command = command("se-zero", "SERVICE", 0);

        assertThrows(IllegalArgumentException.class, () -> service.handleBillingExecute(command, account, account.epoch()));
    }
}
