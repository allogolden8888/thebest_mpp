package uz.mpp.partnersmpp.codec;

/**
 * Декодированный PDU целиком — header + тело. {@code body} — null для
 * header-only PDU (enquire_link, enquire_link_resp, unbind, unbind_resp,
 * generic_nack), иначе один из record-типов в этом пакете
 * ({@link BindTransceiver}, {@link BindTransceiverResp},
 * {@link ShortMessagePdu}, {@link ShortMessagePduResp}).
 */
public record Pdu(PduHeader header, Object body) {

    public static Pdu headerOnly(int commandId, int commandStatus, int sequenceNumber) {
        return new Pdu(new PduHeader(commandId, commandStatus, sequenceNumber), null);
    }

    public static Pdu withBody(int commandId, int commandStatus, int sequenceNumber, Object body) {
        return new Pdu(new PduHeader(commandId, commandStatus, sequenceNumber), body);
    }
}