package uz.mpp.operatorsmpp.core;

/**
 * Разбор SMPP delivery receipt (SMPP 3.4 Appendix B) — вытаскивает
 * {@code smsc_message_id} и статус из текста {@code short_message} у
 * {@code deliver_sm}.
 *
 * <p><b>Реальный баг, ради которого это появилось (найден вживую, не
 * инспекцией):</b> {@code Main.java} собирал {@link
 * uz.mpp.platformcontracts.events.v1.OperatorDlr} вообще без
 * {@code smsc_message_id} — заполнялись только operator_id/protocol/
 * raw_status/received_at. При этом {@code smsc_message_id} — ЕДИНСТВЕННЫЙ
 * ключ корреляции: {@code dlr-manager} джойнит по нему
 * {@code dlr.dlr_correlation} (которую пишет {@code dlr-correlation-writer}
 * из {@code operator.submit.accepted}), чтобы понять, к какому
 * {@code message_id} относится пришедший DLR. Без него dlr-manager
 * логировал "OperatorDlr без smsc_message_id" и отбрасывал КАЖДЫЙ DLR —
 * топик {@code delivery.status} оставался пустым (0 сообщений при 78k
 * реальных DLR в {@code operator.dlr}), {@code message-state-resolver}
 * никогда не получал исход доставки, и ЛЮБОЕ сообщение навсегда
 * оставалось в статусе SUBMITTED.
 *
 * <p>Формат receipt'а (Appendix B, «should be as follows» — не жёсткий
 * ABNF, операторы отклоняются в деталях, поэтому парсер намеренно
 * толерантный):
 * <pre>
 * id:IIIIIIIIII sub:SSS dlvrd:DDD submit date:YYMMDDhhmm done date:YYMMDDhhmm stat:DDDDDDD err:E Text:. . .
 * </pre>
 *
 * <p>Толерантность к реальности вместо строгого соответствия спеке:
 * поля ищутся по префиксу в любом порядке и в любом регистре, лишние/
 * отсутствующие поля не ломают разбор, {@code Text:} может содержать
 * пробелы и сам по себе выглядеть как поле. Единственное, что обязано
 * присутствовать — {@code id:}; всё остальное опционально.
 */
public final class DeliveryReceiptParser {

    private DeliveryReceiptParser() {
    }

    /**
     * Значение поля {@code id:} — SMSC-идентификатор сообщения.
     *
     * @return найденный id, либо пустая строка, если поля нет (вызывающая
     *         сторона решает, что делать — контракт proto требует string,
     *         не optional, поэтому null здесь не возвращается).
     */
    public static String extractSmscMessageId(String rawReceipt) {
        return extractField(rawReceipt, "id:");
    }

    /**
     * Значение поля {@code stat:} — операторский код статуса
     * (DELIVRD/EXPIRED/UNDELIV/REJECTD/...). Нормализацию в платформенный
     * словарь делает dlr-manager, не этот парсер.
     */
    public static String extractStatus(String rawReceipt) {
        return extractField(rawReceipt, "stat:");
    }

    /**
     * Поиск по префиксу, регистронезависимо, значение — до первого
     * пробела. Умышленно не regex: {@code Text:} в конце receipt'а может
     * содержать что угодно, включая последовательности, похожие на другие
     * поля, а линейный поиск по префиксу с явной границей значения не
     * зависит от того, что идёт после.
     */
    private static String extractField(String rawReceipt, String prefix) {
        if (rawReceipt == null || rawReceipt.isEmpty()) {
            return "";
        }
        String lower = rawReceipt.toLowerCase();
        int idx = lower.indexOf(prefix);
        while (idx >= 0) {
            // Префикс должен стоять в начале строки либо после пробела —
            // иначе "id:" совпал бы внутри "msgid:12345" и вернул мусор.
            if (idx == 0 || Character.isWhitespace(rawReceipt.charAt(idx - 1))) {
                int valueStart = idx + prefix.length();
                int valueEnd = valueStart;
                while (valueEnd < rawReceipt.length() && !Character.isWhitespace(rawReceipt.charAt(valueEnd))) {
                    valueEnd++;
                }
                return rawReceipt.substring(valueStart, valueEnd);
            }
            idx = lower.indexOf(prefix, idx + 1);
        }
        return "";
    }
}
