package uz.mpp.billingoutbox.core;

/**
 * Одна запись из billing:outbox:{shard} (data_infrastructure_spec.md §2.3),
 * уже разобранная из полей Redis STREAM entry. Поля пишет Billing Service —
 * этот сервис только читает и не решает, какие поля должны быть, только
 * фиксирует ожидаемую форму (см. README "Открытый вопрос").
 */
public record StreamEntry(
    int shard, // индекс billing:outbox:{shard}, нужен для XACK на правильный ключ
    String redisEntryId, // XREADGROUP entry ID, нужен для XACK
    String chargeId,
    String accountId,
    String partnerId,
    long amountMinorUnits,
    String currencyCode,
    String entryType, // "charge" | "compensating"
    String sourceChargeId, // пусто, если entryType == "charge"
    String reason,
    long createdAtEpochMs
) {
}
