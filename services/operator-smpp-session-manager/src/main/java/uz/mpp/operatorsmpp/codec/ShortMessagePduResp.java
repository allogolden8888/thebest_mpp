package uz.mpp.operatorsmpp.codec;

/** Тело submit_sm_resp/deliver_sm_resp — оба несут только message_id (может быть пустым для deliver_sm_resp). */
public record ShortMessagePduResp(String messageId) {
}