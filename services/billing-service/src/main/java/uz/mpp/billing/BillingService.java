package uz.mpp.billing;

import uz.mpp.billing.BillingAccountState.Account;
import uz.mpp.billing.BillingAccountState.ChargeOutcome;
import uz.mpp.billing.BillingAccountState.ChargeResult;
import uz.mpp.platformcontracts.common.v1.BillingExtension;
import uz.mpp.platformcontracts.common.v1.BillingResult;
import uz.mpp.platformcontracts.common.v1.Money;
import uz.mpp.platformcontracts.common.v1.Outcome;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;

/**
 * {@code handle_billing_execute} (service_internal_methods.md §1.6) —
 * оркестрация {@code read_segment_hint} -> {@code resolve_tariff} ->
 * {@code check_account_state}+{@code check_charge_dedup}+{@code apply_atomic_charge}
 * (три проверки объединены в {@link BillingAccountState#applyCharge}, как в
 * реальном Lua-скрипте) -> {@code publish_stage_completed}.
 *
 * <p><b>Упрощение этого среза, не редизайн:</b> {@code resolve_tariff} по
 * спеке ключуется по {@code partner_id}, которого нет в
 * {@code StageExecuteCommand} напрямую (только {@code stage_extension} с
 * {@code resolved_operator_id}/{@code segment_count}/{@code category}) —
 * вероятный источник, как и для Policy Service, это {@code msgctx} в
 * Runtime Redis. Этот срез соответствует Фазе 2.2 (один тестовый партнёр,
 * один тариф) — {@link TariffResolver} не параметризован по партнёру,
 * реальная per-partner маршрутизация тарифа — открытый пункт, см. README.
 */
public final class BillingService {

    private final TariffResolver tariffResolver;

    public BillingService(TariffResolver tariffResolver) {
        this.tariffResolver = tariffResolver;
    }

    public record Result(StageCompletedEvent event, Account updatedAccount) {
    }

    /**
     * @param account         текущее состояние счёта, как реально стоит в Billing Redis
     *                        прямо перед атомарным вызовом (после fetch, до CAS-записи)
     * @param expectedEpoch   epoch, который вызывающая сторона наблюдала в момент диспетчеризации
     *                        команды — <b>намеренно отдельный параметр, не {@code account.epoch()}</b>:
     *                        если бы epoch читался из того же {@code account}, race "freeze произошёл
     *                        между чтением и записью" был бы структурно недоказуем через этот метод
     *                        (см. {@code BillingAccountStateTest#inFlightChargeRejectedByStaleEpochRace}
     *                        для доказательства свойства на уровне state machine, и
     *                        {@code staleEpochRejectedAsRetryable} здесь — на уровне оркестрации).
     *                        <b>Открытая находка:</b> ни {@code BillingExtension}, ни
     *                        {@code StageExecuteCommand} не несут явного поля под account_epoch —
     *                        вероятный источник, как и {@code partner_id}, конфигурация/Execution
     *                        State, закэшированные Pipeline Engine на момент диспетчеризации
     *                        (hld.md §15.3), не формализовано в текущих platform-contracts — см. README.
     */
    public Result handleBillingExecute(StageExecuteCommand command, Account account, long expectedEpoch) {
        BillingExtension ext = command.getBilling();
        TariffResolver.Tariff tariff = tariffResolver.resolve(ext.getCategory(), ext.getSegmentCount());

        ChargeResult chargeResult = BillingAccountState.applyCharge(
            account, command.getStageExecutionId(), tariff.amountMinorUnits(), expectedEpoch);

        StageCompletedEvent event = switch (chargeResult.outcome()) {
            case APPLIED, ALREADY_PROCESSED -> succeeded(command, ext.getCategory(), tariff);
            case ACCOUNT_FROZEN -> rejectedRetryable(command, "ACCOUNT_FROZEN");
            case STALE_EPOCH -> rejectedRetryable(command, "STALE_EPOCH");
        };

        return new Result(event, chargeResult.account());
    }

    private StageCompletedEvent succeeded(StageExecuteCommand command, String category, TariffResolver.Tariff tariff) {
        BillingResult result = BillingResult.newBuilder()
            .setChargeId(command.getStageExecutionId())
            .setCategory(category)
            .setAmount(Money.newBuilder()
                .setCurrencyCode(tariff.currencyCode())
                .setMinorUnits(tariff.amountMinorUnits())
                .build())
            .build();
        return baseEventBuilder(command)
            .setOutcome(Outcome.OUTCOME_SUCCEEDED)
            .setReasonCode("")
            .setRetryable(false)
            .setBilling(result)
            .build();
    }

    private StageCompletedEvent rejectedRetryable(StageExecuteCommand command, String reasonCode) {
        // ChargeOutcome (ACCOUNT_FROZEN/STALE_EPOCH) оба ретраябельны: замороженный
        // счёт может разморозиться, устаревший epoch — обновиться при повторной
        // попытке (service_internal_methods.md §1.6: "ACCOUNT_FROZEN, retryable=true").
        return baseEventBuilder(command)
            .setOutcome(Outcome.OUTCOME_REJECTED)
            .setReasonCode(reasonCode)
            .setRetryable(true)
            .build();
    }

    private StageCompletedEvent.Builder baseEventBuilder(StageExecuteCommand command) {
        return StageCompletedEvent.newBuilder()
            .setEventId("evt-" + command.getStageExecutionId())
            .setMessageId(command.getMessageId())
            .setStageExecutionId(command.getStageExecutionId())
            .setAttempt(command.getAttempt())
            .setStageName(command.getStageName())
            .setTraceparent(command.getTraceparent());
    }
}
