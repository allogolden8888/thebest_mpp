package uz.mpp.operatorsmpp.core;

import java.time.Instant;

/**
 * Один PDU, реально пересечённый по проводу с оператором — сырое,
 * протокол-нейтральное представление (без protobuf), которое {@code client}/
 * {@code client.OperatorSmppClientHandler} эмитят через {@code pduLogSink},
 * тем же паттерном, что уже применён для {@code dlrSink}
 * ({@link uz.mpp.operatorsmpp.codec.ShortMessagePdu} наружу, а не готовый
 * proto) — построение {@code OperatorPduLog} и публикация в Kafka остаются
 * заботой {@code Main.java}, не низкоуровневого SMPP-кода
 * (BACKOFFICE_DESIGN_SPEC.md Экраны 38-40).
 *
 * @param direction        "A2P" | "DLR"
 * @param pduType          "SUBMIT_SM" | "SUBMIT_SM_RESP" | "DELIVER_SM" | "DELIVER_SM_RESP"
 *                          (см. {@link uz.mpp.operatorsmpp.codec.CommandId#name(int)})
 * @param sequenceNumber   SMPP header sequence_number — связывает submit_sm с
 *                         его submit_sm_resp В РАМКАХ одной живой сессии, не
 *                         глобально уникален.
 * @param messageId        известен для A2P (SubmitRequest уже несёт его),
 *                         пустая строка для DLR — тот же барьер, что уже
 *                         задокументирован у {@code OperatorDlr}.
 * @param stageExecutionId известен для A2P, пустая строка для DLR.
 * @param smscMessageId    заполняется, как только становится известен
 *                         (submit_sm_resp — из тела ответа; deliver_sm — из
 *                         разобранного receipt'а через
 *                         {@link DeliveryReceiptParser}).
 * @param status           "OK"/"0x<hex>" для submit_sm_resp, сырой DLR stat
 *                         для deliver_sm, пусто для submit_sm/deliver_sm_resp.
 * @param occurredAt       момент, когда ЭТОТ PDU реально был отправлен/получен.
 */
public record PduLogEvent(
    String direction,
    String pduType,
    int sequenceNumber,
    String messageId,
    String stageExecutionId,
    String smscMessageId,
    String status,
    Instant occurredAt
) {
    public static final String DIRECTION_A2P = "A2P";
    public static final String DIRECTION_DLR = "DLR";
}
