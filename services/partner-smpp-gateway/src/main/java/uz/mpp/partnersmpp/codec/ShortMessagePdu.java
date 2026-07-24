package uz.mpp.partnersmpp.codec;

/**
 * Общее тело submit_sm/deliver_sm (SMPP 3.4 §4.4.1/§4.6.1 — идентичная
 * структура полей). schedule_delivery_time/validity_period сознательно не
 * представлены отдельными полями — читаются/пишутся как пустые C-strings
 * (обычный случай для immediate delivery без scheduling), см. README
 * "Что НЕ реализовано".
 */
public record ShortMessagePdu(
    String serviceType,
    byte sourceAddrTon,
    byte sourceAddrNpi,
    String sourceAddr,
    byte destAddrTon,
    byte destAddrNpi,
    String destinationAddr,
    byte esmClass,
    byte protocolId,
    byte priorityFlag,
    byte registeredDelivery,
    byte replaceIfPresentFlag,
    byte dataCoding,
    byte smDefaultMsgId,
    byte[] shortMessage
) {
}