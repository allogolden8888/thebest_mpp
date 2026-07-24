package uz.mpp.partnersmpp.codec;

/** SMPP 3.4 PDU header (4 × uint32 big-endian, 16 bytes) — command_length не
 *  хранится здесь, вычисляется PduCodec при кодировании из тела. */
public record PduHeader(int commandId, int commandStatus, int sequenceNumber) {
}