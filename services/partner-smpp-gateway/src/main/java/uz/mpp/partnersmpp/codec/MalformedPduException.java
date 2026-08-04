package uz.mpp.partnersmpp.codec;

/**
 * Закрывает HIGH находку кодревью (CODE_REVIEW.md, "partner-smpp-gateway" #3):
 * раньше {@link SmppStrings#readCString} и неизвестный {@code command_id} в
 * {@link PduCodec#decode} бросали {@link IndexOutOfBoundsException}/
 * {@link IllegalArgumentException} без единого обработчика в пайплайне —
 * исключение уходило в Netty default tail (лог + игнор), партнёр зависал
 * без ответа. Отдельный тип — чтобы {@code SmppServerHandler.channelRead0}
 * мог поймать РОВНО класс "не смогли распарсить PDU" и ответить
 * GENERIC_NACK, не маскируя другие программные ошибки широким {@code catch}.
 */
public final class MalformedPduException extends RuntimeException {
    public MalformedPduException(String message) {
        super(message);
    }
}
