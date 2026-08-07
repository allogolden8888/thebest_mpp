package uz.mpp.billing;

import java.nio.charset.StandardCharsets;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * {@code apply_atomic_charge} для двух видов биллинга, которых нет в
 * per-segment модели (development_plan.md 5.4): ежемесячная плата за
 * зарегистрированный alphaname/short number, и ежемесячный пакет
 * сервисных SMS. Оба — НЕ per-message charge (не привязаны к
 * {@code stage_execution_id}) — {@code StageExecuteCommand}/
 * {@code BillingExtension} не несут {@code partner_id} вообще (см.
 * javadoc {@link BillingService}, "Упрощение этого среза") — поэтому эти
 * charge'и планируются отдельно, вне обычного per-message пути, но
 * применяются той же атомарной операцией
 * ({@link BillingAccountStore#applyChargeAtomically}), что и обычные —
 * тот же {@code charge_id}-дедуп механизм даёт идемпотентность:
 * {@link RecurringBillingJob} может безопасно перезапускаться (например
 * раз в сутки, не строго раз в месяц) — повторный вызов с тем же
 * {@code charge_id} в тот же период просто вернёт
 * {@code ALREADY_PROCESSED}, ничего не спишет дважды.
 *
 * <p><b>Честная граница:</b> "сервисный пакет засчитывается против
 * per-segment SERVICE-начислений, пока не исчерпан 40000" НЕ
 * реализовано — это требовало бы знать {@code partner_id}/пакет-баланс
 * на каждом per-message charge (в {@link BillingService#handleBillingExecute}),
 * а {@code partner_id} туда физически не приходит по проводу в этом
 * срезе (тот же самый, уже задокументированный пробел, не новый).
 * Реализовано ТОЛЬКО начисление ежемесячной платы за сам пакет — то,
 * что реально достижимо без изменения platform-contracts.
 */
public final class RecurringCharges {

    private RecurringCharges() {
    }

    public record Sender(String senderId, String type, String status) {
        public boolean active() {
            return "active".equals(status);
        }
    }

    public record ServicePackage(long segments, long price) {
    }

    public record PlannedCharge(String chargeId, long amount, String description) {
    }

    /**
     * Детерминированный {@code charge_id} — тот же (partnerId, kind,
     * senderIdOrNull, period) ВСЕГДА даёт тот же UUID (UUID v3,
     * {@link UUID#nameUUIDFromBytes} — детерминированный хэш, не
     * случайный {@link UUID#randomUUID()}), так что повторный запуск
     * job'а в тот же период идемпотентен через уже существующий
     * charge_id-дедуп в {@code apply_atomic_charge.lua} — не изобретён
     * новый механизм идемпотентности, переиспользован существующий.
     */
    public static String chargeId(String partnerId, String kind, String senderIdOrNull, YearMonth period) {
        String key = "recurring:" + kind + ":" + partnerId + ":" + (senderIdOrNull == null ? "" : senderIdOrNull) + ":" + period;
        return UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8)).toString();
    }

    /**
     * plan_recurring_charges — чистая функция: текущие senders/тариф/период
     * -> список того, что должно быть списано. Ничего не мутирует, не
     * обращается к Redis — {@link RecurringBillingJob} применяет
     * результат.
     *
     * @param senders             текущий реестр partner.schema.json senders[]
     *                            (archived пропускаются — плата берётся
     *                            только за активные)
     * @param alphanameMonthlyFee null/0 — фича выключена для этого партнёра/тарифа
     * @param servicePackage      null — пакет не куплен/не сконфигурирован
     */
    public static List<PlannedCharge> plan(String partnerId, List<Sender> senders, Long alphanameMonthlyFee, ServicePackage servicePackage, YearMonth period) {
        List<PlannedCharge> charges = new ArrayList<>();

        if (alphanameMonthlyFee != null && alphanameMonthlyFee > 0) {
            for (Sender s : senders) {
                if (!s.active()) {
                    continue;
                }
                charges.add(new PlannedCharge(
                    chargeId(partnerId, "alphaname_fee", s.senderId(), period),
                    alphanameMonthlyFee,
                    "alphaname_monthly_fee sender=%s period=%s".formatted(s.senderId(), period)));
            }
        }

        if (servicePackage != null && servicePackage.price() > 0) {
            charges.add(new PlannedCharge(
                chargeId(partnerId, "service_package", null, period),
                servicePackage.price(),
                "service_sms_package segments=%d period=%s".formatted(servicePackage.segments(), period)));
        }

        return charges;
    }
}
