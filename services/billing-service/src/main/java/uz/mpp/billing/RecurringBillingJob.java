package uz.mpp.billing;

import java.time.Clock;
import java.time.YearMonth;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Оркестрация {@link RecurringCharges#plan} -> {@link BillingAccountStore#applyChargeAtomically}
 * (development_plan.md 5.4). Рассчитан на периодический перезапуск
 * (см. {@link Main} — {@code ScheduledExecutorService}, не точный
 * "раз в календарный месяц" cron) — идемпотентен через charge_id-дедуп,
 * повторный прогон в тот же {@link YearMonth} безопасен.
 *
 * <p>epoch для {@code apply_atomic_charge} читается заново перед КАЖДЫМ
 * charge в списке (не один раз на весь batch) — если один charge
 * реально применился, epoch не меняется (обычные charge не двигают
 * epoch, только freeze/unfreeze — см. {@link BillingAccountState#applyCharge}),
 * но если между charge'ами счёт заморозили, последующие корректно
 * получат ACCOUNT_FROZEN с актуальным epoch, а не STALE_EPOCH с
 * устаревшим.
 */
public final class RecurringBillingJob implements Runnable {

    private static final Logger LOG = Logger.getLogger(RecurringBillingJob.class.getName());

    private final BillingAccountStore accountStore;
    private final String accountId;
    private final String partnerId;
    private final List<RecurringCharges.Sender> senders;
    private final Long alphanameMonthlyFee;
    private final RecurringCharges.ServicePackage servicePackage;
    private final Clock clock;

    public RecurringBillingJob(BillingAccountStore accountStore, String accountId, String partnerId,
                                List<RecurringCharges.Sender> senders, Long alphanameMonthlyFee,
                                RecurringCharges.ServicePackage servicePackage, Clock clock) {
        this.accountStore = accountStore;
        this.accountId = accountId;
        this.partnerId = partnerId;
        this.senders = senders;
        this.alphanameMonthlyFee = alphanameMonthlyFee;
        this.servicePackage = servicePackage;
        this.clock = clock;
    }

    @Override
    public void run() {
        YearMonth period = YearMonth.now(clock);
        List<RecurringCharges.PlannedCharge> charges = RecurringCharges.plan(partnerId, senders, alphanameMonthlyFee, servicePackage, period);
        if (charges.isEmpty()) {
            return;
        }
        LOG.info(() -> "recurring billing tick: %d charge(s) planned for period=%s".formatted(charges.size(), period));

        for (RecurringCharges.PlannedCharge charge : charges) {
            try {
                long expectedEpoch = accountStore.peekEpoch(accountId);
                var result = accountStore.applyChargeAtomically(accountId, charge.chargeId(), charge.amount(), expectedEpoch);
                switch (result.outcome()) {
                    case APPLIED -> LOG.info(() -> "recurring charge applied: " + charge.description());
                    case ALREADY_PROCESSED -> {
                        // Ожидаемо на каждом прогоне после первого успешного в этом
                        // периоде — не ошибка, не логируется на уровне INFO, чтобы
                        // не шуметь при ежедневном перезапуске.
                    }
                    case ACCOUNT_FROZEN, STALE_EPOCH -> LOG.warning(
                        () -> "recurring charge отложен (%s), будет повторён на следующем прогоне: %s".formatted(result.outcome(), charge.description()));
                }
            } catch (RuntimeException e) {
                // Один упавший charge (например транзиентная ошибка Redis) не
                // должен останавливать остальные — каждый charge независим и
                // идемпотентен, следующий прогон подхватит пропущенный.
                LOG.log(Level.WARNING, e, () -> "recurring charge failed, будет повторён на следующем прогоне: " + charge.description());
            }
        }
    }
}
