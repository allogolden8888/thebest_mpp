package uz.mpp.partnersmpp.codec;

/**
 * Подмножество SMPP 3.4 command_id, нужное Partner SMPP Gateway
 * (services_specifictaion.md §2.2: bind/unbind, submit_sm, deliver_sm,
 * enquire_link). Не полный SMPP 3.4 (нет data_sm/replace_sm/cancel_sm/
 * query_sm — Partner-side query_sm читает PostgreSQL read model напрямую,
 * не через SMPP PDU в эту сторону, см. README "Что НЕ реализовано").
 */
public final class CommandId {
    private CommandId() {
    }

    public static final int GENERIC_NACK = 0x80000000;
    public static final int BIND_TRANSCEIVER = 0x00000009;
    public static final int BIND_TRANSCEIVER_RESP = 0x80000009;
    public static final int UNBIND = 0x00000006;
    public static final int UNBIND_RESP = 0x80000006;
    public static final int SUBMIT_SM = 0x00000004;
    public static final int SUBMIT_SM_RESP = 0x80000004;
    public static final int DELIVER_SM = 0x00000005;
    public static final int DELIVER_SM_RESP = 0x80000005;
    public static final int ENQUIRE_LINK = 0x00000015;
    public static final int ENQUIRE_LINK_RESP = 0x80000015;

    public static boolean isResponse(int commandId) {
        return (commandId & 0x80000000) != 0;
    }

    public static String name(int commandId) {
        return switch (commandId) {
            case GENERIC_NACK -> "GENERIC_NACK";
            case BIND_TRANSCEIVER -> "BIND_TRANSCEIVER";
            case BIND_TRANSCEIVER_RESP -> "BIND_TRANSCEIVER_RESP";
            case UNBIND -> "UNBIND";
            case UNBIND_RESP -> "UNBIND_RESP";
            case SUBMIT_SM -> "SUBMIT_SM";
            case SUBMIT_SM_RESP -> "SUBMIT_SM_RESP";
            case DELIVER_SM -> "DELIVER_SM";
            case DELIVER_SM_RESP -> "DELIVER_SM_RESP";
            case ENQUIRE_LINK -> "ENQUIRE_LINK";
            case ENQUIRE_LINK_RESP -> "ENQUIRE_LINK_RESP";
            default -> "UNKNOWN(0x" + Integer.toHexString(commandId) + ")";
        };
    }
}