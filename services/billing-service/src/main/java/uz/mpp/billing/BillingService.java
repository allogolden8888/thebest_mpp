package uz.mpp.billing;

import uz.mpp.billing.BillingAccountState.Account;
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
 * -> {@code publish_stage_completed}.
 *
 * <p>Тариф резолвится и валидируется здесь ({@link #resolveTariff}), сама
 * атомарная мутация счёта — в {@link BillingAccountStore#applyChargeAtomically}
 * (WATCH/MULTI/EXEC), не в этом классе — {@link #handleBillingExecute}
 * остаётся чистой in-memory версией для юнит-тестов (использует
 * {@link BillingAccountState#applyCharge} напрямую, без Redis), но
 * пропускает вход через ту же {@link #resolveTariff} валидацию, что и
 * реальный Kafka-путь — негативный {@code segment_count} отклоняется
 * одинаково в обоих местах, не только в одном.
 *
 * <p><b>Фаза 5a плана закрытия API-пробелов закрыла упрощение "Фазы 2.2"
 * выше</b> (текст оставлен для истории — раньше {@link TariffResolver} не
 * был параметризован по партнёру вообще): {@code BillingExtension} теперь
 * несёт {@code partner_id} (накоплен Pipeline Engine с самого начала
 * обработки сообщения, не перечитывается из {@code msgctx} — Billing
 * намеренно не читает Runtime Redis per-message ради throughput). Per-partner
 * резолв тарифа — {@link TariffCache}, вызывающая сторона ({@link KafkaIo})
 * передаёт уже резолвленный {@link TariffResolver} в {@link #resolveTariff}.
 * Конструкторный {@code tariffResolver} остаётся дефолтом только для
 * {@link #handleBillingExecute} (чистый in-memory путь юнит-тестов).
 */
public final class BillingService {

    private final TariffResolver tariffResolver;

    public BillingService(TariffResolver tariffResolver) {
        this.tariffResolver = tariffResolver;
    }

    public record Result(StageCompletedEvent event, Account updatedAccount) {
    }

    /**
     * Отклоняет отрицательный/нулевой {@code segment_count} до того, как он
     * дойдёт до арифметики списания — найдено кодревью: непровалидированный
     * отрицательный {@code segment_count} (сырой wire {@code int32},
     * `stage_contract.proto`) инвертирует списание в начисление
     * ({@code perSegment * segmentCount} с отрицательным множителем), и
     * {@code applyCharge} рапортует это как обычный {@code SUCCEEDED}.
     */
    public TariffResolver.Tariff resolveTariff(TariffResolver resolver, BillingExtension ext) {
        if (ext.getSegmentCount() <= 0) {
            throw new IllegalArgumentException(
                "segment_count обязан быть положительным, получено " + ext.getSegmentCount()
                    + " — отрицательное/нулевое значение инвертировало бы списание в начисление");
        }
        return resolver.resolve(ext.getCategory(), ext.getSegmentCount());
    }

    public StageCompletedEvent buildEvent(StageExecuteCommand command, String category, TariffResolver.Tariff tariff, ChargeResult chargeResult) {
        return switch (chargeResult.outcome()) {
            case APPLIED, ALREADY_PROCESSED -> succeeded(command, category, tariff);
            case ACCOUNT_FROZEN -> rejectedRetryable(command, "ACCOUNT_FROZEN");
            case STALE_EPOCH -> rejectedRetryable(command, "STALE_EPOCH");
        };
    }

    /**
     * Чистая in-memory версия для юнит-тестов — эквивалент реального пути
     * (resolveTariff -> BillingAccountState.applyCharge -> buildEvent), но
     * без Redis. См. {@link BillingAccountStore#applyChargeAtomically} для
     * того, что реально исполняется в проде (KafkaIo).
     *
     * @param expectedEpoch epoch, который вызывающая сторона наблюдала в момент
     *                      диспетчеризации команды — намеренно отдельный параметр,
     *                      не {@code account.epoch()}, см. предыдущую версию javadoc
     *                      в истории коммитов для полного обоснования (race-доказательство
     *                      в {@code staleEpochRejectedAsRetryable}).
     */
    public Result handleBillingExecute(StageExecuteCommand command, Account account, long expectedEpoch) {
        BillingExtension ext = command.getBilling();
        TariffResolver.Tariff tariff = resolveTariff(tariffResolver, ext);
        ChargeResult chargeResult = BillingAccountState.applyCharge(account, command.getStageExecutionId(), tariff.amountMinorUnits(), expectedEpoch);
        StageCompletedEvent event = buildEvent(command, ext.getCategory(), tariff, chargeResult);
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
            .setTraceparent(command.getTraceparent())
            // Фаза 11 плана закрытия API-пробелов: эхо command.sandbox —
            // общий "конверт" StageCompletedEvent несёт флаг независимо от
            // outcome (succeeded/rejected), чтобы message-state-resolver
            // видел его даже при отклонённом заряде.
            .setSandbox(command.getSandbox());
    }
}
